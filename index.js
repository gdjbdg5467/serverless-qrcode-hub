let KV_BINDING;
let DB;
const banPath = [
  'login', 'admin', '__total_count',
  // static files
  'admin.html', 'login.html',
  'daisyui@5.css', 'tailwindcss@4.js',
  'qr-code-styling.js', 'zxing.js',
  'robots.txt', 'wechat.svg',
  'favicon.svg',
];

// Telegram Bot 相关配置
const TG_API_BASE = "https://api.telegram.org/bot";

// 数据库初始化
async function initDatabase() {
  // 创建表
  await DB.prepare(`
    CREATE TABLE IF NOT EXISTS mappings (
      path TEXT PRIMARY KEY,
      target TEXT NOT NULL,
      name TEXT,
      expiry TEXT,
      enabled INTEGER DEFAULT 1,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
  `).run();

  // 检查是否需要添加新列
  const tableInfo = await DB.prepare("PRAGMA table_info(mappings)").all();
  const columns = tableInfo.results.map(col => col.name);

  // 添加 isWechat 列（如果不存在）
  if (!columns.includes('isWechat')) {
    await DB.prepare(`
      ALTER TABLE mappings 
      ADD COLUMN isWechat INTEGER DEFAULT 0
    `).run();
  }

  // 添加 qrCodeData 列（如果不存在）
  if (!columns.includes('qrCodeData')) {
    await DB.prepare(`
      ALTER TABLE mappings 
      ADD COLUMN qrCodeData TEXT
    `).run();
  }

  // 添加索引
  await DB.prepare(`
    CREATE INDEX IF NOT EXISTS idx_expiry ON mappings(expiry)
  `).run();

  await DB.prepare(`
    CREATE INDEX IF NOT EXISTS idx_created_at ON mappings(created_at)
  `).run();

  // 组合索引：用于启用状态和过期时间的组合查询
  await DB.prepare(`
    CREATE INDEX IF NOT EXISTS idx_enabled_expiry ON mappings(enabled, expiry)
  `).run();
}

// Cookie 相关函数
function verifyAuthCookie(request, env) {
  const cookie = request.headers.get('Cookie') || '';
  const authToken = cookie.split(';').find(c => c.trim().startsWith('token='));
  if (!authToken) return false;
  return authToken.split('=')[1].trim() === env.PASSWORD;
}

function setAuthCookie(password) {
  return {
    'Set-Cookie': `token=${password}; Path=/; HttpOnly; SameSite=Strict; Max-Age=86400`,
    'Content-Type': 'application/json'
  };
}

function clearAuthCookie() {
  return {
    'Set-Cookie': 'token=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0',
    'Content-Type': 'application/json'
  };
}

// 数据库操作相关函数
async function listMappings(page = 1, pageSize = 10) {
  const offset = (page - 1) * pageSize;
  
  // 使用单个查询获取分页数据和总数
  const results = await DB.prepare(`
    WITH filtered_mappings AS (
      SELECT * FROM mappings 
      WHERE path NOT IN (${banPath.map(() => '?').join(',')})
    )
    SELECT 
      filtered.*,
      (SELECT COUNT(*) FROM filtered_mappings) as total_count
    FROM filtered_mappings as filtered
    ORDER BY created_at DESC
    LIMIT ? OFFSET ?
  `).bind(...banPath, pageSize, offset).all();

  if (!results.results || results.results.length === 0) {
    return {
      mappings: {},
      total: 0,
      page,
      pageSize,
      totalPages: 0
    };
  }

  const total = results.results[0].total_count;
  const mappings = {};

  for (const row of results.results) {
    mappings[row.path] = {
      target: row.target,
      name: row.name,
      expiry: row.expiry,
      enabled: row.enabled === 1,
      isWechat: row.isWechat === 1,
      qrCodeData: row.qrCodeData
    };
  }

  return {
    mappings,
    total,
    page,
    pageSize,
    totalPages: Math.ceil(total / pageSize)
  };
}

async function createMapping(path, target, name, expiry, enabled = true, isWechat = false, qrCodeData = null) {
  if (!path || !target || typeof path !== 'string' || typeof target !== 'string') {
    throw new Error('Invalid input');
  }

  // 检查短链名是否在禁用列表中
  if (banPath.includes(path)) {
    throw new Error('该短链名已被系统保留，请使用其他名称');
  }

  if (expiry && isNaN(Date.parse(expiry))) {
    throw new Error('Invalid expiry date');
  }

  // 如果是微信二维码，必须提供二维码数据
  if (isWechat && !qrCodeData) {
    throw new Error('微信二维码必须提供原始二维码数据');
  }

  await DB.prepare(`
    INSERT INTO mappings (path, target, name, expiry, enabled, isWechat, qrCodeData)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `).bind(
    path,
    target,
    name || null,
    expiry || null,
    enabled ? 1 : 0,
    isWechat ? 1 : 0,
    qrCodeData
  ).run();
}

async function deleteMapping(path) {
  if (!path || typeof path !== 'string') {
    throw new Error('Invalid input');
  }

  // 检查是否在禁用列表中
  if (banPath.includes(path)) {
    throw new Error('系统保留的短链名无法删除');
  }

  await DB.prepare('DELETE FROM mappings WHERE path = ?').bind(path).run();
}

async function updateMapping(originalPath, newPath, target, name, expiry, enabled = true, isWechat = false, qrCodeData = null) {
  if (!originalPath || !newPath || !target) {
    throw new Error('Invalid input');
  }

  // 检查新短链名是否在禁用列表中
  if (banPath.includes(newPath)) {
    throw new Error('该短链名已被系统保留，请使用其他名称');
  }

  if (expiry && isNaN(Date.parse(expiry))) {
    throw new Error('Invalid expiry date');
  }

  // 如果没有提供新的二维码数据，获取原有的二维码数据
  if (!qrCodeData && isWechat) {
    const existingMapping = await DB.prepare(`
      SELECT qrCodeData
      FROM mappings
      WHERE path = ?
    `).bind(originalPath).first();

    if (existingMapping) {
      qrCodeData = existingMapping.qrCodeData;
    }
  }

  // 如果是微信二维码，必须有二维码数据
  if (isWechat && !qrCodeData) {
    throw new Error('微信二维码必须提供原始二维码数据');
  }

  const stmt = DB.prepare(`
    UPDATE mappings 
    SET path = ?, target = ?, name = ?, expiry = ?, enabled = ?, isWechat = ?, qrCodeData = ?
    WHERE path = ?
  `);

  await stmt.bind(
    newPath,
    target,
    name || null,
    expiry || null,
    enabled ? 1 : 0,
    isWechat ? 1 : 0,
    qrCodeData,
    originalPath
  ).run();
}

async function getExpiringMappings() {
  // 获取今天的日期（设置为今天的23:59:59）
  const today = new Date();
  today.setHours(23, 59, 59, 999);
  const now = today.toISOString();
  
  // 获取今天的开始时间（00:00:00）
  const todayStart = new Date();
  todayStart.setHours(0, 0, 0, 0);
  const dayStart = todayStart.toISOString();
  
  // 修改为3天后的23:59:59
  const threeDaysFromNow = new Date(todayStart);
  threeDaysFromNow.setDate(todayStart.getDate() + 3);
  threeDaysFromNow.setHours(23, 59, 59, 999);
  const threeDaysLater = threeDaysFromNow.toISOString();

  // 使用单个查询获取所有过期和即将过期的映射
  const results = await DB.prepare(`
    WITH categorized_mappings AS (
      SELECT 
        path, name, target, expiry, enabled, isWechat, qrCodeData,
        CASE 
          WHEN datetime(expiry) < datetime(?) THEN 'expired'
          WHEN datetime(expiry) <= datetime(?) THEN 'expiring'
        END as status
      FROM mappings 
      WHERE expiry IS NOT NULL 
        AND datetime(expiry) <= datetime(?) 
        AND enabled = 1
    )
    SELECT * FROM categorized_mappings
    ORDER BY expiry ASC
  `).bind(dayStart, threeDaysLater, threeDaysLater).all();

  const mappings = {
    expiring: [],
    expired: []
  };
  
  for (const row of results.results) {
    const mapping = {
      path: row.path,
      name: row.name,
      target: row.target,
      expiry: row.expiry,
      enabled: row.enabled === 1,
      isWechat: row.isWechat === 1,
      qrCodeData: row.qrCodeData
    };

    if (row.status === 'expired') {
      mappings.expired.push(mapping);
    } else {
      mappings.expiring.push(mapping);
    }
  }

  return mappings;
}

// 批量清理过期映射的函数
async function cleanupExpiredMappings(batchSize = 100) {
  const now = new Date().toISOString();
  
  while (true) {
    // 获取一批过期的映射
    const batch = await DB.prepare(`
      SELECT path 
      FROM mappings 
      WHERE expiry IS NOT NULL 
        AND expiry < ? 
      LIMIT ?
    `).bind(now, batchSize).all();

    if (!batch.results || batch.results.length === 0) {
      break;
    }

    // 批量删除这些映射
    const paths = batch.results.map(row => row.path);
    const placeholders = paths.map(() => '?').join(',');
    await DB.prepare(`
      DELETE FROM mappings 
      WHERE path IN (${placeholders})
    `).bind(...paths).run();

    // 如果获取的数量小于 batchSize，说明已经处理完所有过期映射
    if (batch.results.length < batchSize) {
      break;
    }
  }
}

// 数据迁移函数
async function migrateFromKV() {
  let cursor = null;
  do {
    const listResult = await KV_BINDING.list({ cursor, limit: 1000 });
    
    for (const key of listResult.keys) {
      if (!banPath.includes(key.name)) {
        const value = await KV_BINDING.get(key.name, { type: "json" });
        if (value) {
          try {
            await createMapping(
              key.name,
              value.target,
              value.name,
              value.expiry,
              value.enabled,
              value.isWechat,
              value.qrCodeData
            );
          } catch (e) {
            console.error(`Failed to migrate ${key.name}:`, e);
          }
        }
      }
    }
    
    cursor = listResult.cursor;
  } while (cursor);
}

// Telegram Bot 工具函数
async function sendTgMessage(env, chatId, text, replyToMessageId = null) {
  const url = `${TG_API_BASE}${env.TG_BOT_TOKEN}/sendMessage`;
  const params = {
    chat_id: chatId,
    text: text,
    parse_mode: "Markdown",
    ...(replyToMessageId && { reply_to_message_id: replyToMessageId })
  };

  return fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(params)
  });
}

async function sendTgPhoto(env, chatId, photoData, caption, replyToMessageId = null) {
  const url = `${TG_API_BASE}${env.TG_BOT_TOKEN}/sendPhoto`;
  const formData = new FormData();
  
  formData.append('chat_id', chatId);
  formData.append('photo', photoData);
  formData.append('caption', caption || '');
  if (replyToMessageId) {
    formData.append('reply_to_message_id', replyToMessageId);
  }

  return fetch(url, {
    method: "POST",
    body: formData
  });
}

// 生成随机短链路径
function generateRandomPath(length = 6) {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  return Array.from({ length }, () => chars[Math.floor(Math.random() * chars.length)]).join("");
}

// 生成二维码
async function generateQrCode(url) {
  const qrcode = await import('qrcode');
  return new Promise((resolve, reject) => {
    qrcode.toDataURL(url, (err, dataUrl) => {
      if (err) reject(err);
      else resolve(dataUrl);
    });
  });
}

// 处理 Telegram 消息
async function handleTgUpdate(env, update) {
  const message = update.message;
  if (!message) return;

  const chatId = message.chat.id;
  const text = message.text || message.caption; // 支持文本和媒体描述
  const replyToId = message.message_id;

  // 处理命令
  if (text?.startsWith('/')) {
    if (text === '/start' || text === '/help') {
      return sendTgMessage(
        env,
        chatId,
        "👋 欢迎使用短链二维码生成机器人！\n\n请发送包含链接的消息（例如：https://example.com），我会为您生成短链和二维码。\n支持在群组中@我处理链接。",
        replyToId
      );
    }
    return sendTgMessage(env, chatId, "未知命令，请发送链接生成短链或使用 /help 查看帮助", replyToId);
  }

  // 验证权限（仅管理员或已登录用户可使用）
  if (env.TG_ADMIN_ID && chatId.toString() !== env.TG_ADMIN_ID) {
    return sendTgMessage(env, chatId, "❌ 您没有权限使用此功能", replyToId);
  }

  // 提取链接（支持直接链接、转发的链接）
  const urlMatch = text?.match(/https?:\/\/\S+/);
  if (!urlMatch) {
    return sendTgMessage(env, chatId, "请发送包含链接的消息（例如：https://example.com）", replyToId);
  }

  const targetUrl = urlMatch[0];
  const path = generateRandomPath(); // 生成随机短链路径

  try {
    // 生成二维码
    const qrCodeDataUrl = await generateQrCode(targetUrl);
    const qrCodeBuffer = Buffer.from(qrCodeDataUrl.split(',')[1], 'base64');
    
    // 创建短链
    await createMapping(
      path,
      targetUrl,
      `TG-${new Date().toISOString().slice(0, 10)}`, // 名称包含日期
      null, // 永不过期
      true, // 启用
      false, // 非微信二维码
      qrCodeDataUrl
    );

    const shortUrl = `${new URL(env.ORIGIN).origin}/${path}`;
    
    // 发送二维码和短链
    await sendTgPhoto(
      env,
      chatId,
      new Blob([qrCodeBuffer], { type: 'image/png' }),
      `✅ 短链生成成功：\n${shortUrl}\n\n点击直接访问`,
      replyToId
    );
  } catch (error) {
    return sendTgMessage(env, chatId, `❌ 生成失败：${error.message}`, replyToId);
  }
}

export default {
  async fetch(request, env) {
    KV_BINDING = env.KV_BINDING;
    DB = env.DB;
    
    // 初始化数据库
    await initDatabase();
    
    const url = new URL(request.url);
    const path = url.pathname.slice(1);

    // 处理 Telegram Bot 回调
    if (path === `bot${env.TG_BOT_TOKEN}`) {
      if (request.method === "POST") {
        const update = await request.json();
        await handleTgUpdate(env, update);
        return new Response(JSON.stringify({ ok: true }));
      }
      // 验证 TG Bot 回调（GET 请求用于设置 Webhook 验证）
      const challenge = url.searchParams.get("hub.challenge");
      return new Response(challenge || "OK");
    }

    // 根目录跳转到管理后台
    if (path === '') {
      return Response.redirect(url.origin + '/admin.html', 302);
    }

    // API 路由处理
    if (path.startsWith('api/')) {
      // 登录 API
      if (path === 'api/login' && request.method === 'POST') {
        const { password } = await request.json();
        if (password === env.PASSWORD) {
          return new Response(JSON.stringify({ success: true }), {
            headers: setAuthCookie(password)
          });
        } else {
          return new Response(JSON.stringify({ success: false, message: '密码错误' }), {
            status: 401,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }

      // 登出 API
      if (path === 'api/logout' && request.method === 'POST') {
        return new Response(JSON.stringify({ success: true }), {
          headers: clearAuthCookie()
        });
      }

      // 验证权限
      if (!verifyAuthCookie(request, env)) {
        return new Response(JSON.stringify({ success: false, message: '未授权' }), {
          status: 401,
          headers: { 'Content-Type': 'application/json' }
        });
      }

      // 短链列表 API
      if (path === 'api/mappings' && request.method === 'GET') {
        const page = parseInt(url.searchParams.get('page') || '1');
        const pageSize = parseInt(url.searchParams.get('pageSize') || '10');
        const result = await listMappings(page, pageSize);
        return new Response(JSON.stringify(result), {
          headers: { 'Content-Type': 'application/json' }
        });
      }

      // 创建短链 API
      if (path === 'api/mappings' && request.method === 'POST') {
        const { path, target, name, expiry, enabled, isWechat, qrCodeData } = await request.json();
        try {
          await createMapping(path, target, name, expiry, enabled, isWechat, qrCodeData);
          return new Response(JSON.stringify({ success: true }), {
            headers: { 'Content-Type': 'application/json' }
          });
        } catch (error) {
          return new Response(JSON.stringify({ success: false, message: error.message }), {
            status: 400,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }

      // 删除短链 API
      if (path.startsWith('api/mappings/') && request.method === 'DELETE') {
        const mappingPath = path.split('api/mappings/')[1];
        try {
          await deleteMapping(mappingPath);
          return new Response(JSON.stringify({ success: true }), {
            headers: { 'Content-Type': 'application/json' }
          });
        } catch (error) {
          return new Response(JSON.stringify({ success: false, message: error.message }), {
            status: 400,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }

      // 更新短链 API
      if (path.startsWith('api/mappings/') && request.method === 'PUT') {
        const originalPath = path.split('api/mappings/')[1];
        const { path: newPath, target, name, expiry, enabled, isWechat, qrCodeData } = await request.json();
        try {
          await updateMapping(originalPath, newPath, target, name, expiry, enabled, isWechat, qrCodeData);
          return new Response(JSON.stringify({ success: true }), {
            headers: { 'Content-Type': 'application/json' }
          });
        } catch (error) {
          return new Response(JSON.stringify({ success: false, message: error.message }), {
            status: 400,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }

      // 获取即将过期的短链
      if (path === 'api/mappings/expiring' && request.method === 'GET') {
        const result = await getExpiringMappings();
        return new Response(JSON.stringify(result), {
          headers: { 'Content-Type': 'application/json' }
        });
      }

      // 迁移数据 API
      if (path === 'api/migrate' && request.method === 'POST') {
        try {
          await migrateFromKV();
          return new Response(JSON.stringify({ success: true }), {
            headers: { 'Content-Type': 'application/json' }
          });
        } catch (error) {
          return new Response(JSON.stringify({ success: false, message: error.message }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
          });
        }
      }

      // 未找到的 API
      return new Response(JSON.stringify({ success: false, message: 'API 不存在' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' }
      });
    }

    // 处理静态资源
    if (banPath.includes(path)) {
      const asset = await env.ASSETS.get(path);
      if (asset) {
        const contentType = path.endsWith('.html') ? 'text/html' :
                          path.endsWith('.css') ? 'text/css' :
                          path.endsWith('.js') ? 'application/javascript' :
                          path.endsWith('.svg') ? 'image/svg+xml' :
                          'application/octet-stream';
        return new Response(asset, {
          headers: { 'Content-Type': contentType }
        });
      }
    }

    // 处理短链跳转
    const mapping = await DB.prepare('SELECT target FROM mappings WHERE path = ? AND enabled = 1 AND (expiry IS NULL OR expiry > ?)').bind(path, new Date().toISOString()).first();
    if (mapping) {
      return Response.redirect(mapping.target, 302);
    }

    // 404 页面
    return new Response('Not found', { status: 404 });
  },

  // 定时任务处理
  async scheduled(event, env, ctx) {
    KV_BINDING = env.KV_BINDING;
    DB = env.DB;
    await initDatabase();
    await cleanupExpiredMappings();
  }
};
