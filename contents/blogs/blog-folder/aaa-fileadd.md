# read.html 文章附件写法说明

如果文章需要展示关联文件，可以把文件放在当前文章同名文件夹里，例如：

```text
contents/blogs/blog-folder/文章id/估值模型.xlsx
contents/blogs/blog-folder/文章id/完整报告.pdf
contents/blogs/blog-folder/文章id/演示材料.pptx
```

然后在对应的 Markdown 正文里插入文件卡片。

## 单个文件

```markdown
[估值模型](file:估值模型.xlsx "Excel测算底稿")
```

渲染时会自动指向：

```text
./blog-folder/当前文章id/估值模型.xlsx
```

## 多个文件

````markdown
```files
估值模型.xlsx | 估值模型 | Excel测算底稿
完整报告.pdf | 完整报告 | PDF版本
演示材料.pptx | 路演PPT | PPT附件
```
````

每一行格式为：

```text
文件路径 | 显示标题 | 文件说明
```

## 路径规则

- `file:估值模型.xlsx`：默认读取 `blog-folder/当前文章id/估值模型.xlsx`
- `/contents/blogs/blog-folder/文章id/估值模型.xlsx`：也可以使用完整站内路径
- `./blog-folder/文章id/估值模型.xlsx`：也可以使用相对路径

## 支持类型

- Excel：`xls`、`xlsx`、`xlsm`、`csv`
- Word：`doc`、`docx`
- PDF：`pdf`
- PowerPoint：`ppt`、`pptx`
- 压缩包：`zip`、`rar`、`7z`

所有文件都会在插入位置显示一次，并在文章结尾的“关联文件”区域自动汇总一次。每个文件都有“打开”和“下载”按钮。
