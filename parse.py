import requests

# Replace with your local MediaWiki URL and port
base_url = "http://localhost:8080/api.php"

wikitext = """
=== 研究方法 ===
从[[克洛德·贝尔纳]]与{{le|沃尔特·布拉福德·坎农|Walter Bradford Cannon}}揭示生物的稳态现象、[[诺伯特·维纳]]与{{le|威廉·罗斯·艾什比|William Ross Ashby}}的[[控制论]]。
"""

expand_params = {
    "action": "expandtemplates",
    "format": "json",
    "prop": "wikitext",
    "text": wikitext,
    "includecomments": False,
}

response = requests.get(base_url, params=expand_params)

expanded_wikitext = response.json().get('expandtemplates', {}).get('wikitext', None)

print(expanded_wikitext)

parse_params = {
    "action": "parse",
    "format": "json",
    "prop": "text",
    "contentmodel": "wikitext",
    "uselang": "zh-tw",
    "text": expanded_wikitext
}

response = requests.post(base_url, params=parse_params)

# Extract HTML from JSON response
html_text = response.json().get('parse', {}).get('text', {}).get('*', None)

print(html_text)
