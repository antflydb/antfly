(() => {
  const btn = document.getElementById("ping-btn");
  const output = document.getElementById("ping-output");
  if (!btn || !output) return;

  btn.addEventListener("click", () => {
    const now = new Date().toLocaleTimeString();
    output.textContent = `Served by httpx.zig at ${now}`;
  });
})();
