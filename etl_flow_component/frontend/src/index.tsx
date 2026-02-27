import React from "react";
import ReactDOM from "react-dom/client";
import ETLFlowMap from "./ETLFlowMap";

const root = ReactDOM.createRoot(
  document.getElementById("root") as HTMLElement
);

root.render(
  <React.StrictMode>
    <ETLFlowMap />
  </React.StrictMode>
);
