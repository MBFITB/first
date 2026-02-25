import { createApp } from 'vue'
import App from './App.vue'

// 1. 引入 Element Plus 框架和它的 CSS
import ElementPlus from 'element-plus'
import 'element-plus/dist/index.css'

// 2. 引入所有图标并全局注册（这样你的侧边栏图标才能显示）
import * as ElementPlusIconsVue from '@element-plus/icons-vue'

const app = createApp(App)

// 注册所有图标
for (const [key, component] of Object.entries(ElementPlusIconsVue)) {
  app.component(key, component)
}

app.use(ElementPlus)
app.mount('#app')