# send_push_events.py
# ------------------
# 读取未推送事件并执行推送，再更新状态

import pandas as pd
from event_logger import EVENTS_FILE, update_push_status
from feishu_bot import FeishuBot
# 如果需要发送邮件，可自行导入你的 send_email 函数
# from email_sender import send_email

# 请替换为你的飞书 Webhook 地址
FEISHU_WEBHOOK = FEISHU_WEBHOOK_URL


def send_pending_events(sender_email=None, sender_password=None, receivers=None):
    """
    读取 CSV 中 push_status 为 未推送 的事件，
    逐个执行推送并更新状态
    sender_email, sender_password, receivers: 如果需要邮件推送，可传入对应参数
    """
    df = pd.read_csv(EVENTS_FILE, dtype=str)
    pending = df[df['push_status'] == '未推送']
    if pending.empty:
        print("No pending events to push.")
        return

    bot = FeishuBot(FEISHU_WEBHOOK)
    for _, row in pending.iterrows():
        event_id     = row['id']
        subject      = row['related_subject']
        title        = row['report_title']
        details      = row['report_details']
        large_status = row['large_status']
        try:
            # 可选：邮件推送
            # send_email(sender_email, sender_password, receivers, details)

            # 飞书推送卡片消息
            bot.send_card_message(
                content=details,
                title=f"{large_status} - {title}",
                tag_text=large_status,
                tag_color=("green" if large_status == "成功推送"
                           else "yellow" if large_status == "警告推送"
                           else "red"),
                template_color=("green" if large_status == "成功推送"
                                else "yellow" if large_status == "警告推送"
                                else "red")
            )
            # 更新状态为已推送
            update_push_status(event_id, push_status="已推送")
            print(f"Event {event_id} pushed successfully.")
        except Exception as e:
            # 异常也标记为已推送，但大状态改为“异常推送”
            update_push_status(event_id, push_status="已推送", large_status="异常推送")
            # 发送错误卡片
            bot.send_card_message(
                content=f"推送失败：{str(e)}",
                title="🚨 推送异常",
                tag_text="错误",
                tag_color="red",
                template_color="red"
            )
            print(f"Error pushing event {event_id}: {e}")


if __name__ == "__main__":
    send_pending_events()