<template>
  <el-drawer
    v-model="visible"
    title="AI 数据查询助手"
    direction="rtl"
    size="520px"
    @closed="handleClosed"
    custom-class="ai-drawer"
  >
    <div class="chat-container">
      <!-- 消息列表区 -->
      <div class="message-list" ref="messageListRef">
        
        <!-- 欢迎消息 -->
        <div class="message-item assistant">
          <div class="avatar"><el-icon><Monitor /></el-icon></div>
          <div class="bubble">
            <div class="text">你好！我是电商数据大屏的 AI 助手。你可以用自然语言向我提问，例如：<br>
              <br>• <span class="suggestion" @click="useSuggestion('上周五各个渠道的销售额分别是多少？')">“上周五各个渠道的销售额分别是多少？”</span>
              <br>• <span class="suggestion" @click="useSuggestion('销量排名前10的商品是哪些？')">“销量排名前10的商品是哪些？”</span>
              <br>• <span class="suggestion" @click="useSuggestion('25-34岁的用户贡献了多少销售额？')">“25-34岁的用户贡献了多少销售额？”</span>
            </div>
          </div>
        </div>

        <!-- 对话流 -->
        <div 
          v-for="(msg, index) in messages" 
          :key="index" 
          :class="['message-item', msg.role]"
        >
          <div class="avatar">
            <el-icon v-if="msg.role === 'assistant'"><Monitor /></el-icon>
            <el-icon v-else><User /></el-icon>
          </div>
          <div class="bubble-wrapper">
            <div class="bubble" :class="{ error: msg.isError }">
              <div v-if="msg.role === 'assistant'" class="md-content" v-html="renderMarkdown(msg.content)"></div>
              <div v-else class="text">{{ msg.content }}</div>
            </div>
            
            <!-- 如果有表格数据，渲染隐藏/展开的结果表 -->
            <div v-if="msg.data && msg.data.length > 0" class="data-table-container">
              <div class="sql-code" v-if="msg.sql">
                <code>{{ msg.sql }}</code>
              </div>
              <el-table 
                :data="msg.data" 
                size="small" 
                stripe 
                border 
                style="width: 100%; margin-top: 10px;"
                max-height="250"
              >
                <el-table-column 
                  v-for="col in Object.keys(msg.data[0])" 
                  :key="col" 
                  :prop="col" 
                  :label="col" 
                  show-overflow-tooltip
                />
              </el-table>
            </div>
          </div>
        </div>

        <!-- Loading 动画 -->
        <div v-if="loading" class="message-item assistant typing">
          <div class="avatar"><el-icon><Loading class="is-loading"/></el-icon></div>
          <div class="bubble">
            <div class="typing-indicator">
              <span></span><span></span><span></span>
            </div>
          </div>
        </div>
      </div>

      <!-- 底部输入框 -->
      <div class="input-area">
        <el-input
          v-model="inputText"
          type="textarea"
          :rows="2"
          placeholder="输入你想查询的数据问题，按 Enter 发送..."
          @keydown.enter.prevent="sendMessage"
        />
        <el-button 
          type="primary" 
          circle 
          class="send-btn" 
          :icon="Position" 
          :loading="loading"
          @click="sendMessage"
        />
      </div>
    </div>
  </el-drawer>
</template>

<script setup>
import { ref, computed, nextTick, watch } from 'vue'
import { Monitor, User, Position, Loading } from '@element-plus/icons-vue'
import axios from 'axios'
import { marked } from 'marked'

// 配置 marked
marked.setOptions({
  breaks: true,
  gfm: true,
})

// Markdown 渲染辅助函数
const renderMarkdown = (text) => {
  if (!text) return ''
  return marked.parse(text)
}

const props = defineProps({
  modelValue: {
    type: Boolean,
    default: false
  }
})

const emit = defineEmits(['update:modelValue'])

const visible = computed({
  get: () => props.modelValue,
  set: (val) => emit('update:modelValue', val)
})

const inputText = ref('')
const loading = ref(false)
const messages = ref([])
const messageListRef = ref(null)

// 滚动到底部
const scrollToBottom = async () => {
  await nextTick()
  if (messageListRef.value) {
    messageListRef.value.scrollTop = messageListRef.value.scrollHeight
  }
}

watch(messages, () => {
  scrollToBottom()
}, { deep: true })

const handleClosed = () => {
  // 抽屉关闭时的清理，此处可以保持对话记录不断
}

const useSuggestion = (text) => {
  inputText.value = text
}

const sendMessage = async () => {
  const text = inputText.value.trim()
  if (!text || loading.value) return

  messages.value.push({ role: 'user', content: text })
  inputText.value = ''
  loading.value = true
  scrollToBottom()

  const historyPayload = messages.value
    .slice(0, -1)
    .map(m => ({ role: m.role, content: m.content, data: m.data }))

  try {
    const res = await axios.post('http://127.0.0.1:8000/api/ai/chat', {
      question: text,
      history: historyPayload
    }, {
      headers: {
        'Authorization': `Bearer ${localStorage.getItem('token')}`
      }
    })

    const responseData = res.data.data
    messages.value.push({
      role: 'assistant',
      content: responseData.answer || '查询成功',
      sql: responseData.sql,
      data: responseData.data || [],
      isError: !!responseData.error
    })
  } catch (error) {
    console.error('AI查询失败:', error)
    const errObj = error.response?.data?.data || {}
    messages.value.push({
      role: 'assistant',
      content: errObj.answer || error.response?.data?.message || '网络异常，请稍后再试。',
      isError: true
    })
  } finally {
    loading.value = false
    scrollToBottom()
  }
}
</script>

<style scoped>
/* 整个抽屉覆盖默认 padding，实现全屏内部布局 */
:deep(.el-drawer__body) {
  padding: 0;
  display: flex;
  flex-direction: column;
  background-color: #f7f9fc;
}
:deep(.el-drawer__header) {
  margin-bottom: 0;
  padding: 16px 20px;
  background: #0A192F;
  color: white;
  border-bottom: 1px solid rgba(255,255,255,0.1);
}
:deep(.el-drawer__title) {
  color: #fff;
  font-weight: 600;
  letter-spacing: 1px;
}
:deep(.el-drawer__close-btn) {
  color: #fff;
}

.chat-container {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.message-list {
  flex: 1;
  overflow-y: auto;
  padding: 20px;
  scroll-behavior: smooth;
}

.message-item {
  display: flex;
  align-items: flex-start;
  margin-bottom: 24px;
}

.message-item.user {
  flex-direction: row-reverse;
}

.avatar {
  width: 36px;
  height: 36px;
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  color: white;
  flex-shrink: 0;
  font-size: 20px;
}

.assistant .avatar {
  background: linear-gradient(135deg, #38bdf8, #0ea5e9);
  margin-right: 12px;
}

.user .avatar {
  background: #cbd5e1;
  margin-left: 12px;
}

.bubble-wrapper {
  max-width: 85%;
  display: flex;
  flex-direction: column;
}

.user .bubble-wrapper {
  align-items: flex-end;
}

.bubble {
  padding: 12px 16px;
  border-radius: 12px;
  line-height: 1.5;
  font-size: 14px;
  color: #334155;
  word-wrap: break-word;
  box-shadow: 0 2px 6px rgba(0,0,0,0.04);
}

.assistant .bubble {
  background: white;
  border-top-left-radius: 4px;
}

.user .bubble {
  background: #dbeafe;
  color: #1e40af;
  border-top-right-radius: 4px;
}

.bubble.error {
  background: #fee2e2;
  color: #b91c1c;
  border: 1px solid #fca5a5;
}

.suggestion {
  color: #0ea5e9;
  cursor: pointer;
  text-decoration: underline;
  text-decoration-color: transparent;
  transition: all 0.2s;
}
.suggestion:hover {
  text-decoration-color: #0ea5e9;
}

.data-table-container {
  margin-top: 12px;
  background: white;
  border-radius: 8px;
  padding: 12px;
  box-shadow: 0 2px 8px rgba(0,0,0,0.05);
  border: 1px solid #e2e8f0;
}

.sql-code {
  background: #1e293b;
  color: #38bdf8;
  padding: 8px 12px;
  border-radius: 6px;
  font-family: monospace;
  font-size: 12px;
  margin-bottom: 10px;
  word-break: break-all;
}

.input-area {
  padding: 16px 20px;
  background: white;
  border-top: 1px solid #e2e8f0;
  display: flex;
  align-items: flex-end;
  gap: 12px;
}

:deep(.el-textarea__inner) {
  background: #f8fafc;
  border: 1px solid #cbd5e1;
  box-shadow: none !important;
  border-radius: 8px;
  resize: none;
}
:deep(.el-textarea__inner:focus) {
  border-color: #38bdf8;
  background: white;
}

.send-btn {
  height: 48px;
  width: 48px;
  flex-shrink: 0;
  font-size: 20px;
}

/* 打字机动画 */
.typing-indicator {
  display: flex;
  gap: 4px;
  align-items: center;
  height: 20px;
}
.typing-indicator span {
  width: 6px;
  height: 6px;
  background: #94a3b8;
  border-radius: 50%;
  animation: typing 1.4s infinite ease-in-out both;
}
.typing-indicator span:nth-child(1) { animation-delay: -0.32s; }
.typing-indicator span:nth-child(2) { animation-delay: -0.16s; }
@keyframes typing {
  0%, 80%, 100% { transform: scale(0); opacity: 0.5; }
  40% { transform: scale(1); opacity: 1; }
}

/* Markdown 渲染样式 */
.md-content {
  font-size: 14px;
  line-height: 1.7;
  color: #334155;
}
.md-content :deep(h1),
.md-content :deep(h2),
.md-content :deep(h3),
.md-content :deep(h4) {
  margin: 12px 0 6px;
  font-weight: 700;
  color: #1e293b;
}
.md-content :deep(h1) { font-size: 16px; }
.md-content :deep(h2) { font-size: 15px; border-bottom: 1px solid #e2e8f0; padding-bottom: 4px; }
.md-content :deep(h3) { font-size: 14px; color: #0ea5e9; }
.md-content :deep(h4) { font-size: 13px; }
.md-content :deep(p) {
  margin: 6px 0;
}
.md-content :deep(ul),
.md-content :deep(ol) {
  margin: 6px 0;
  padding-left: 20px;
}
.md-content :deep(li) {
  margin: 3px 0;
}
.md-content :deep(strong) {
  color: #0f172a;
  font-weight: 600;
}
.md-content :deep(code) {
  background: #f1f5f9;
  color: #e11d48;
  padding: 1px 5px;
  border-radius: 4px;
  font-size: 12px;
  font-family: 'Consolas', monospace;
}
.md-content :deep(pre) {
  background: #1e293b;
  color: #e2e8f0;
  padding: 10px 14px;
  border-radius: 8px;
  overflow-x: auto;
  margin: 8px 0;
  font-size: 12px;
}
.md-content :deep(pre code) {
  background: none;
  color: #e2e8f0;
  padding: 0;
}
.md-content :deep(blockquote) {
  border-left: 3px solid #38bdf8;
  padding-left: 12px;
  margin: 8px 0;
  color: #64748b;
  font-style: italic;
}
.md-content :deep(table) {
  width: 100%;
  border-collapse: collapse;
  margin: 8px 0;
  font-size: 12px;
}
.md-content :deep(th),
.md-content :deep(td) {
  border: 1px solid #e2e8f0;
  padding: 4px 8px;
  text-align: left;
}
.md-content :deep(th) {
  background: #f1f5f9;
  font-weight: 600;
}
.md-content :deep(hr) {
  border: none;
  border-top: 1px solid #e2e8f0;
  margin: 10px 0;
}
</style>
