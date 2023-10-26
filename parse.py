import requests

# Replace with your local MediaWiki URL and port
base_url = "http://localhost:8080/api.php"

params = {
    "action": "parse",
    "format": "json",
    "contentmodel": "wikitext",
    "uselang": "zh-tw",
    "text": "=== 自由軟體与开源软件 ===\n"
    "斯托曼是一名坚定的自由软件运动倡导者，与提倡[[开放源代码]]开发模型的人不同，斯托曼并不是从软件的-{zh-hans:质量; zh-hant:品質}-的角度而是从道德的角度来看待自由软件。"
}

response = requests.get(base_url, params=params)

# Extract HTML from JSON response
html_text = response.json().get('parse', {}).get('text', {}).get('*', None)

print(html_text)
