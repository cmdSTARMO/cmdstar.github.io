# feishu_bot.py
# -------------
# 飞书机器人辅助类，封装 send_card_message 方法
import requests as rq
import json
from datetime import datetime

class FeishuBot:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

    def send_card_message(self, content, title="通知", tag_text="🔔通知", tag_color="indigo", template_color="blue"):
        headers = {"Content-Type": "application/json; charset=utf-8"}
        card_content = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": title},
                "subtitle": {"tag": "plain_text", "content": datetime.now().strftime('%Y-%m-%d %H:%M:%S')},
                "text_tag_list": [{"tag": "text_tag", "text": {"tag": "plain_text", "content": tag_text}, "color": tag_color}],
                "template": template_color
            },
            "elements": [{"tag": "div", "text": {"tag": "lark_md", "content": content}}]
        }
        payload = {"msg_type": "interactive", "card": card_content}
        resp = rq.post(self.webhook_url, data=json.dumps(payload), headers=headers)
        return resp.json()
