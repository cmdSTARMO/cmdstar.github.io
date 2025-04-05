import os
import re
import shutil
import urllib.parse

def rename_images_in_md(md_file, target_folder):
    print(f"读取 Markdown 文件：{md_file}")
    try:
        with open(md_file, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"读取文件出错：{e}")
        return

    # 如果目标文件夹不存在，则创建
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)
        print(f"创建目标文件夹：{target_folder}")

    counter = 1

    def replace_image(match):
        nonlocal counter
        alt_text = match.group(1)
        old_path_encoded = match.group(2)
        # 对 URL 编码的文件路径进行解码
        old_path = urllib.parse.unquote(old_path_encoded)
        print(f"匹配到图片：alt='{alt_text}', path='{old_path}'")
        # 获取文件扩展名（保留后缀）
        _, ext = os.path.splitext(old_path)
        new_filename = f"img_{counter}{ext}"
        new_link = f"contents/blogs/blog-folder/exchange_hedge_pics/{new_filename}"
        # 如果图片文件存在，则移动并重命名
        if os.path.exists(old_path):
            try:
                shutil.move(old_path, os.path.join(target_folder, new_filename))
                print(f"移动文件：{old_path} -> {os.path.join(target_folder, new_filename)}")
            except Exception as e:
                print(f"移动文件出错 {old_path}：{e}")
        else:
            print(f"警告：文件 {old_path} 不存在")
        counter += 1
        # 返回新的 Markdown 图片语法
        return f"![{alt_text}]({new_link})"

    # 匹配 Markdown 图片语法 ![alt](路径)
    pattern = r"!\[(.*?)\]\((.*?)\)"
    new_content = re.sub(pattern, replace_image, content)

    new_md_file = os.path.splitext(md_file)[0] + "_renamed.md"
    try:
        with open(new_md_file, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"处理完成！新的 Markdown 文件为：{new_md_file}")
    except Exception as e:
        print(f"保存文件出错：{e}")

def main():
    md_file = input("请输入 Markdown 文件路径（例如 exchange_hedge.md）：").strip()
    target_folder = os.path.join("contents", "blogs", "blog-folder", "exchange_hedge_pics")
    rename_images_in_md(md_file, target_folder)

if __name__ == '__main__':
    main()
