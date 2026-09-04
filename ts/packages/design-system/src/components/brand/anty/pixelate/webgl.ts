/**
 * Minimal raw-WebGL helper: a single full-screen-quad fragment-shader runner.
 * No dependency. Used by the pixelate subsystem to render the ripple+pixelate
 * shader over a snapshot texture of the mark.
 *
 * Straight (non-premultiplied) alpha so the canvas overlays the page exactly
 * like the vector SVG it replaces.
 */

const VERT = `
attribute vec2 a;
varying vec2 uv;
void main(){ uv = a*0.5+0.5; gl_Position = vec4(a,0.0,1.0); }`;

export interface QuadProgram {
  setFloat(name: string, v: number): void;
  uploadTexture(src: TexImageSource): void;
  hasTexture(): boolean;
  draw(): void;
  resize(w: number, h: number): void;
  destroy(): void;
}

/**
 * Create a quad program on `canvas` with the given fragment shader source.
 * Returns `null` if WebGL is unavailable or the program fails to build.
 */
export function createQuadProgram(canvas: HTMLCanvasElement, fragSrc: string): QuadProgram | null {
  const gl = canvas.getContext("webgl", {
    premultipliedAlpha: false,
    alpha: true,
    antialias: false,
  });
  if (!gl) return null;

  const compile = (type: number, src: string): WebGLShader | null => {
    const sh = gl.createShader(type);
    if (!sh) return null;
    gl.shaderSource(sh, src);
    gl.compileShader(sh);
    if (!gl.getShaderParameter(sh, gl.COMPILE_STATUS)) {
      console.warn("[anty/pixelate] shader compile failed:", gl.getShaderInfoLog(sh));
      return null;
    }
    return sh;
  };

  const vs = compile(gl.VERTEX_SHADER, VERT);
  const fs = compile(gl.FRAGMENT_SHADER, fragSrc);
  if (!vs || !fs) return null;

  const prog = gl.createProgram();
  if (!prog) return null;
  gl.attachShader(prog, vs);
  gl.attachShader(prog, fs);
  gl.linkProgram(prog);
  if (!gl.getProgramParameter(prog, gl.LINK_STATUS)) {
    console.warn("[anty/pixelate] program link failed:", gl.getProgramInfoLog(prog));
    return null;
  }
  // biome-ignore lint/correctness/useHookAtTopLevel: gl.useProgram is the WebGL API, not a React hook
  gl.useProgram(prog);

  const buf = gl.createBuffer();
  gl.bindBuffer(gl.ARRAY_BUFFER, buf);
  gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([-1, -1, 1, -1, -1, 1, 1, 1]), gl.STATIC_DRAW);
  const aLoc = gl.getAttribLocation(prog, "a");
  gl.enableVertexAttribArray(aLoc);
  gl.vertexAttribPointer(aLoc, 2, gl.FLOAT, false, 0, 0);

  gl.enable(gl.BLEND);
  gl.blendFuncSeparate(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA, gl.ONE, gl.ONE_MINUS_SRC_ALPHA);
  gl.clearColor(0, 0, 0, 0);

  const uniforms = new Map<string, WebGLUniformLocation | null>();
  const loc = (name: string): WebGLUniformLocation | null => {
    if (!uniforms.has(name)) uniforms.set(name, gl.getUniformLocation(prog, name));
    return uniforms.get(name) ?? null;
  };

  let tex: WebGLTexture | null = null;

  return {
    setFloat(name, v) {
      gl.uniform1f(loc(name), v);
    },
    uploadTexture(src) {
      if (!tex) tex = gl.createTexture();
      gl.bindTexture(gl.TEXTURE_2D, tex);
      gl.pixelStorei(gl.UNPACK_PREMULTIPLY_ALPHA_WEBGL, false);
      gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, src);
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
      gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
    },
    hasTexture() {
      return tex !== null;
    },
    draw() {
      // biome-ignore lint/correctness/useHookAtTopLevel: gl.useProgram is the WebGL API, not a React hook
      gl.useProgram(prog);
      gl.viewport(0, 0, canvas.width, canvas.height);
      gl.clear(gl.COLOR_BUFFER_BIT);
      if (tex) {
        gl.activeTexture(gl.TEXTURE0);
        gl.bindTexture(gl.TEXTURE_2D, tex);
        gl.uniform1i(loc("tex"), 0);
      }
      gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
    },
    resize(w, h) {
      if (canvas.width !== w) canvas.width = w;
      if (canvas.height !== h) canvas.height = h;
    },
    destroy() {
      gl.getExtension("WEBGL_lose_context")?.loseContext();
    },
  };
}
