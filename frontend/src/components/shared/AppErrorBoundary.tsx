import React from "react";

interface AppErrorBoundaryState {
  hasError: boolean;
  message: string;
  details: string;
}

export class AppErrorBoundary extends React.Component<
  React.PropsWithChildren,
  AppErrorBoundaryState
> {
  state: AppErrorBoundaryState = {
    hasError: false,
    message: "",
    details: "",
  };

  static getDerivedStateFromError(error: unknown): AppErrorBoundaryState {
    const message = error instanceof Error ? error.message : "Unknown frontend error";
    const stack = error instanceof Error && error.stack ? error.stack : "";
    return {
      hasError: true,
      message,
      details: stack,
    };
  }

  componentDidCatch(error: unknown, info: React.ErrorInfo): void {
    console.error("AppErrorBoundary caught an error", error, info);
    this.setState((prev) => ({
      ...prev,
      details: [prev.details, info.componentStack].filter(Boolean).join("\n\n"),
    }));
  }

  render() {
    if (this.state.hasError) {
      return (
        <div
          style={{
            minHeight: "100vh",
            margin: 0,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            background: "#0f172a",
            color: "#e2e8f0",
            fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
            padding: "24px",
          }}
        >
          <div style={{ maxWidth: "900px", width: "100%" }}>
            <h1 style={{ margin: "0 0 12px 0", fontSize: "22px" }}>
              Frontend runtime error
            </h1>
            <p style={{ margin: "0 0 12px 0", opacity: 0.9 }}>
              The app crashed while rendering. Reload the page once. If it still fails,
              send this message to support.
            </p>
            <pre
              style={{
                whiteSpace: "pre-wrap",
                wordBreak: "break-word",
                background: "#111827",
                border: "1px solid #334155",
                borderRadius: "8px",
                padding: "12px",
                margin: 0,
              }}
            >
{this.state.message || "No error message provided."}
            </pre>
            {this.state.details ? (
              <pre
                style={{
                  whiteSpace: "pre-wrap",
                  wordBreak: "break-word",
                  background: "#0b1220",
                  border: "1px solid #334155",
                  borderRadius: "8px",
                  padding: "12px",
                  marginTop: "12px",
                  opacity: 0.95,
                  maxHeight: "45vh",
                  overflow: "auto",
                }}
              >
{this.state.details}
              </pre>
            ) : null}
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}
