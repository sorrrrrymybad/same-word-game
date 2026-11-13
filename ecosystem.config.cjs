// Author: Sorrymybad
module.exports = {
  apps: [{
    name: 'same-word-game',
    cwd: __dirname,
    script: 'server.js',
    // 房间状态在内存中，必须使用单进程。
    instances: 1,
    exec_mode: 'fork',
    env: {
      NODE_ENV: 'production',
      PORT: 3001,
      TRUST_PROXY: '1', // 仅信任本机 Nginx 转发的访客 IP
    },
  }],
};
