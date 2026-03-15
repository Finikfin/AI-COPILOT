import { ENDPOINTS } from '@/constants/api';
import { AuthResponse } from '@/types/auth';

export interface LoginRequest {
  email: string;
  password: string;
}

export interface RegisterRequest {
  email: string;
  fullName: string;
  password: string;
}

/**
 * Log in to the application
 * @param data Login credentials
 * @returns Auth response with user data and token
 */
export const login = async (data: LoginRequest): Promise<AuthResponse> => {
  const response = await fetch(ENDPOINTS.AUTH.LOGIN, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(data),
  });

  if (!response.ok) {
    const errorData = await response.json();
    throw new Error(errorData.detail?.message || 'Ошибка входа');
  }

  return response.json();
};

/**
 * Register a new user
 * @param data Registration data
 * @returns Auth response with user data and token
 */
export const register = async (data: RegisterRequest): Promise<AuthResponse> => {
  const response = await fetch(ENDPOINTS.AUTH.REGISTER, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(data),
  });

  if (!response.ok) {
    const errorData = await response.json();
    throw new Error(errorData.detail?.message || 'Ошибка регистрации');
  }

  return response.json();
};
