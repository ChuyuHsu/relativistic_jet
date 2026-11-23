// 權杖比對：長度不同也走完整比對（常數時間，避免靠回應時間猜權杖）。
export function safeEqual(a, b) {
  a = String(a); b = String(b);
  let d = a.length ^ b.length;
  const n = Math.max(a.length, b.length);
  for (let i = 0; i < n; i++) d |= (a.charCodeAt(i) || 0) ^ (b.charCodeAt(i) || 0);
  return d === 0;
}
// 請求帶的 Bearer 權杖是否等於任一把非空的權杖
export function bearerIs(req, ...tokens) {
  const h = req.headers.get('authorization') || '';
  const got = h.startsWith('Bearer ') ? h.slice(7) : null;
  let ok = false;
  for (const t of tokens) if (t && got !== null && safeEqual(got, t)) ok = true;
  return ok;
}
