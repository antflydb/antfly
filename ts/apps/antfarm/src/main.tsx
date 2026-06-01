import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import "./global.css";
import App from "./App.tsx";
import { getAntfarmBasePath } from "./runtime-config";

const rootElement = document.getElementById("root");
if (!rootElement) throw new Error("Root element not found");

createRoot(rootElement).render(
  <StrictMode>
    <BrowserRouter basename={getAntfarmBasePath()}>
      <App />
    </BrowserRouter>
  </StrictMode>
);
