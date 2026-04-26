// use in console on page directly to check

(() => {
  const text = document.body.innerText;
  const contentSize = text.length;

  let tokens = [];
  let cur = "";

  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (/^[a-z0-9]$/i.test(c)) {
      cur += c.toLowerCase();
    } else {
      if (cur) {
        tokens.push(cur);
        cur = "";
      }
    }
  }
  if (cur) tokens.push(cur);

  console.log("Content size (chars):", contentSize);
  console.log("Token count:", tokens.length);
  console.log("Sample tokens:", tokens.slice(0, 20));
})();