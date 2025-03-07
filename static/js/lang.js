// lang.js
// 用于初始化语言切换控件，并将用户的选择保存在 localStorage 中

(function() {
  // 初始化语言切换逻辑
  function initLanguageToggle() {
    const langToggle = document.getElementById('langToggle');
    // 获取本地存储的语言设置，默认使用中文 'zh'
    const storedLang = localStorage.getItem('lang') || 'zh';
    // 设置页面语言
    document.documentElement.lang = storedLang;

    if (langToggle) {
      // 根据存储的值设置切换开关状态
      langToggle.checked = (storedLang === 'en');

      // 绑定切换事件
      langToggle.addEventListener('change', function() {
        if (this.checked) {
          document.documentElement.lang = 'en';
          localStorage.setItem('lang', 'en');
          console.log("Switched to English");
        } else {
          document.documentElement.lang = 'zh';
          localStorage.setItem('lang', 'zh');
          console.log("切换到中文");
        }
      });
    } else {
      console.warn("未找到语言切换控件 #langToggle");
    }
  }

  // 将 initLanguageToggle 函数暴露到全局对象中，以便在其他地方调用
  window.initLanguageToggle = initLanguageToggle;
})();