import React, { createContext, useContext, useEffect, useState } from "react";
import { User, AuthState } from "@/types/auth";
import * as authApi from "@/api/auth";

interface AuthContextType extends AuthState {
  login: (email: string, password: string) => Promise<void>;
  register: (
    email: string,
    fullName: string,
    password: string,
  ) => Promise<void>;
  logout: () => void;
  updateUser: (user: Partial<User>) => void;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export const AuthProvider: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  const [authState, setAuthState] = useState<AuthState>({
    user: null,
    isAuthenticated: false,
    token: null,
  });
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    try {
      const storedToken = localStorage.getItem("auth_token");
      const storedUser = localStorage.getItem("user_data");

      if (storedToken && storedUser) {
        setAuthState({
          user: JSON.parse(storedUser),
          isAuthenticated: true,
          token: storedToken,
        });
      }
    } catch (error) {
      console.error("Failed to parse stored auth data", error);
      localStorage.removeItem("auth_token");
      localStorage.removeItem("user_data");
    } finally {
      setIsLoading(false);
    }
  }, []);

  const login = async (email: string, password: string) => {
    const response = await authApi.login({ email, password });
    const { accessToken, user } = response;

    setAuthState({
      user,
      isAuthenticated: true,
      token: accessToken,
    });

    localStorage.setItem("auth_token", accessToken);
    localStorage.setItem("user_data", JSON.stringify(user));
  };

  const register = async (
    email: string,
    fullName: string,
    password: string,
  ) => {
    const response = await authApi.register({
      email,
      fullName,
      password,
    });
    const { accessToken, user } = response;

    setAuthState({
      user,
      isAuthenticated: true,
      token: accessToken,
    });

    localStorage.setItem("auth_token", accessToken);
    localStorage.setItem("user_data", JSON.stringify(user));
  };

  const logout = () => {
    setAuthState({
      user: null,
      isAuthenticated: false,
      token: null,
    });
    localStorage.removeItem("auth_token");
    localStorage.removeItem("user_data");
    localStorage.removeItem("auth_token");
    localStorage.removeItem("user_data");
  };

  const updateUser = (userData: Partial<User>) => {
    if (!authState.user) {
      return;
    }

    const updatedUser = { ...authState.user, ...userData };
    setAuthState((prev) => ({ ...prev, user: updatedUser }));
    localStorage.setItem("user_data", JSON.stringify(updatedUser));
  };

  if (isLoading) {
    return null;
  }

  if (isLoading) {
    return null;
  }

  return (
    <AuthContext.Provider
      value={{
        ...authState,
        login,
        register,
        logout,
        updateUser,
      }}
    >
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
};
