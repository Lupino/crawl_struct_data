from urllib.parse import urlparse
from grapy import BaseSpider, Request
from ..items import TripleItem, tripleItem
import asyncio
import json
import os


def api(uri):
    return 'https://api.github.com/repos/{}'.format(uri)


repo_keys = [
    'homepage', 'language', 'license.name', 'description', 'default_branch'
]


def get_meta(meta, keys):
    retval = {}
    for key in keys:
        ks = key.split('.')
        v = meta.get(ks[0])
        for k in ks[1:]:
            if v:
                v = v.get(k)

        retval[ks[0]] = v

    return retval


def jsonToTriple(entity, data):
    for k, v in data.items():
        if v == "":
            continue
        if v == "None":
            continue
        if v is None:
            continue

        yield tripleItem(entity, k, v)


def gen_download_url(repo, name):
    download_url = 'https://api.github.com/repos/{}/tarball/{}'.format(
        repo, name)
    return {'tag_name': name, 'download_url': download_url}


def get_file_name(url):
    p = urlparse(url)
    fn = p.path.replace('/', '_')
    if fn.find('tarball') > -1:
        fn += '.tar.gz'

    return fn


class GithubSpider(BaseSpider):
    name = "github"

    def setup(self, config):
        self.token = config.get('token')
        self.download_path = config['download_path']

        os.makedirs(self.download_path, exist_ok=True)

        for repo in config['repos']:
            entity = repo.pop('repo')
            yield Request(api(entity),
                          callback='parse_meta',
                          callback_args=[entity],
                          headers=self._get_headers())

            yield Request(api(entity + '/tags'),
                          callback='parse_tag_meta',
                          callback_args=[entity, repo['version']],
                          headers=self._get_headers())

            # if isinstance(repo['version'], list):
            #     for v in repo['version']:
            #         yield tripleItem(entity, 'version', v)
            # else:
            #     yield tripleItem(entity, 'version', repo['version'])

            # yield tripleItem(entity, 'spec', repo['spec'])

    def _get_headers(self):
        if self.token:
            return {'Authorization': 'token {}'.format(self.token)}
        return {}

    def parse_meta(self, rsp, entity):
        info = get_meta(rsp.json(), repo_keys)
        return jsonToTriple(entity, info)

    def parse_tag_meta(self, rsp, entity, tag_names):
        found = False
        meta = rsp.json()
        if isinstance(tag_names, str):
            tag_names = [tag_names]
        elif not isinstance(tag_names, list):
            tag_names = [str(tag_names)]
        if len(meta) > 0:
            if not tag_names:
                tag_names = [meta[0]['name']]

            for m in meta:
                if not m.get('name'):
                    continue
                for tag_name in tag_names:
                    if m['name'].find(tag_name) > -1:
                        yield tripleItem(entity, 'tag_name', m['name'])
                        yield tripleItem(entity, 'download_url',
                                         m['tarball_url'])
                        yield Request(m['tarball_url'],
                                      callback='parse_cloc',
                                      headers=self._get_headers(),
                                      callback_args=[entity])
                        found = True

        if not found:
            for tag_name in tag_names:
                yield Request(api(entity + '/commits/' + tag_name),
                              callback='parse_commit_meta',
                              callback_args=[entity, tag_name],
                              headers=self._get_headers())

    def parse_commit_meta(self, rsp, entity, commit):
        if rsp.json().get('message'):
            yield Request(api(entity + '/branches/' + commit),
                          callback='parse_branch_meta',
                          callback_args=[entity, commit],
                          headers=self._get_headers())

        else:
            meta = gen_download_url(entity, commit)
            yield Request(meta['download_url'],
                          callback='parse_cloc',
                          headers=self._get_headers(),
                          callback_args=[entity])
            yield from jsonToTriple(entity, meta)

    def parse_branch_meta(self, rsp, entity, branch):
        meta = {}
        if rsp.json().get('message'):
            meta = gen_download_url(entity, 'master')
        else:
            meta = gen_download_url(entity, branch)

        yield Request(meta['download_url'],
                      callback='parse_cloc',
                      headers=self._get_headers(),
                      callback_args=[entity])

        yield from jsonToTriple(entity, meta)

    async def parse_cloc(self, rsp, entity, next):
        fn = '{}/{}'.format(self.download_path, get_file_name(rsp.req.url))
        with open(fn, 'wb') as f:
            f.write(rsp.content)

        # Create the subprocess; redirect the standard output
        # into a pipe.
        proc = await asyncio.create_subprocess_exec(
            'cloc', '--json', fn, stdout=asyncio.subprocess.PIPE)

        data = await proc.stdout.read()

        await proc.wait()

        retval = json.loads(str(data, 'utf-8'))

        retval.pop('header', None)
        sum = retval.pop('SUM')
        v0 = 0
        lang = ''
        for k, v in retval.items():
            if v['code'] > v0:
                v0 = v['code']
                lang = k

        await next(tripleItem(entity, 'main_language', lang))
        await next(tripleItem(entity, 'code', str(sum['code'])))
