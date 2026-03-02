import React from "react";
import ReactDOM from "react-dom/client";
import ERDFlowMap from "./ERDFlowMap";

const root = ReactDOM.createRoot(
  document.getElementById("root") as HTMLElement
);

root.render(
  <React.StrictMode>
    <ERDFlowMap />
  </React.StrictMode>
);
