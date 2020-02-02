# 知识图普爬虫

用于抓取三元组的爬虫

## 安装依赖

```
pip install -r requirements.txt
```

## 运行

```
python main.py resources/ffmpeg.yml
```


## 目录结构

```
.
├── config.py                       - 常规配置文件
├── main.py                         - 入口文件
├── requirements.txt                - 依赖文件
├── resources                       - 知识的配置目录
│   └── ffmpeg.yml                  - ffmpeg 配置文件
└── triple                          - 项目目录
    ├── __init__.py
    ├── items.py                    - 定义 TripleItem
    ├── middlewares.py              - 中间件
    ├── pipelines.py                - 抓到的 TripleItem 处理
    ├── spiders                     - 爬虫目录
    │   ├── __init__.py
    │   └── ffmpeg.py               - ffmpeg 爬虫
    └── utils.py                    - 公共文件

```
