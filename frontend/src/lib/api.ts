const AUTH_TOKEN_KEY = "auth_token";
const USER_DATA_KEY = "user_data";

const clearAuthState = () => {
  localStorage.removeItem(AUTH_TOKEN_KEY);
  localStorage.removeItem(USER_DATA_KEY);
};

const isAuthRequest = (url: string) => {
  return url.includes("/auth/login") || url.includes("/auth/register");
};

const extractErrorMessage = (errorData: unknown): string | null => {
  if (!errorData || typeof errorData !== "object") {
    return null;
  }

  const payload = errorData as {
    message?: unknown;
    detail?: unknown;
  };

  if (typeof payload.message === "string" && payload.message.trim()) {
    return payload.message;
  }

  if (typeof payload.detail === "string" && payload.detail.trim()) {
    return payload.detail;
  }

  if (
    payload.detail &&
    typeof payload.detail === "object" &&
    "message" in payload.detail &&
    typeof (payload.detail as { message?: unknown }).message === "string"
  ) {
    const detailMessage = (payload.detail as { message: string }).message;
    if (detailMessage.trim()) {
      return detailMessage;
    }
  }

  return null;
};

/**
 * Utility for making authenticated API requests
 */
export async function apiRequest<T>(
  url: string,
  options: RequestInit = {},
): Promise<T> {
  const token = localStorage.getItem(AUTH_TOKEN_KEY);

  const headers = new Headers(options.headers || {});
  if (token) {
    headers.set("Authorization", `Bearer ${token}`);
  }
  if (!headers.has("Content-Type") && !(options.body instanceof FormData)) {
    headers.set("Content-Type", "application/json");
  }

  const response = await fetch(url, {
    ...options,
    headers,
  });

  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    const errorMessage = extractErrorMessage(errorData) || "API request failed";

    if (response.status === 401 && !isAuthRequest(url)) {
      clearAuthState();
      if (window.location.pathname !== "/login") {
        window.location.assign("/login");
      }
      throw new Error("Session expired. Please sign in again.");
    }

    throw new Error(errorMessage);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return response.json();
}
