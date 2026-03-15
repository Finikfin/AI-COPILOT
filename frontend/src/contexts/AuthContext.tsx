import React, { createContext, useContext, useState, useEffect } from 'react';
import { User, AuthState } from '@/types/auth';
import * as authApi from '@/api/auth';

/**
 * Extended authentication context type that includes authentication methods
 */
interface AuthContextType extends AuthState {
  login: (email: string, password: string) => Promise<void>;
  register: (email: string, fullName: string, password: string) => Promise<void>;
  logout: () => void;
  updateUser: (user: Partial<User>) => void;
}

/**
 * Authentication context for managing user authentication state
 */
const AuthContext = createContext<AuthContextType | undefined>(undefined);

/**
 * AuthProvider component
 * 
 * Provides authentication context to the application. Manages user
 * authentication state, login/logout functionality, and user data updates.
 */
export const AuthProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  /**
   * Authentication state including user data, authentication status, and token
   */
  const [authState, setAuthState] = useState<AuthState>({
    user: null,
    isAuthenticated: false,
    token: null
  });

  const [isLoading, setIsLoading] = useState(true);

  /**
   * Initialize authentication state on component mount
   * Checks for stored token and user data in localStorage
   */
  useEffect(() => {
    const initAuth = () => {
      try {
        const storedToken = localStorage.getItem('auth_token');
        const storedUser = localStorage.getItem('user_data');

        if (storedToken && storedUser) {
          setAuthState({
            user: JSON.parse(storedUser),
            isAuthenticated: true,
            token: storedToken
          });
        }
      } catch (error) {
        console.error('Failed to parse stored auth data', error);
        localStorage.removeItem('auth_token');
        localStorage.removeItem('user_data');
      } finally {
        setIsLoading(false);
      }
    };

    initAuth();
  }, []);

  /**
   * Real login function
   * @param email User email
   * @param password User password
   */
  const login = async (email: string, password: string) => {
    const response = await authApi.login({ email, password });
    
    const { accessToken, user } = response;
    
    setAuthState({
      user,
      isAuthenticated: true,
      token: accessToken
    });
    
    localStorage.setItem('auth_token', accessToken);
    localStorage.setItem('user_data', JSON.stringify(user));
  };

  /**
   * Real register function
   * @param email User email
   * @param fullName User full name
   * @param password User password
   */
  const register = async (email: string, fullName: string, password: string) => {
    const response = await authApi.register({ email, fullName, password });
    
    const { accessToken, user } = response;
    
    setAuthState({
      user,
      isAuthenticated: true,
      token: accessToken
    });
    
    localStorage.setItem('auth_token', accessToken);
    localStorage.setItem('user_data', JSON.stringify(user));
  };

  /**
   * Logout function
   * Clears session data and state
   */
  const logout = () => {
    setAuthState({
      user: null,
      isAuthenticated: false,
      token: null
    });
    localStorage.removeItem('auth_token');
    localStorage.removeItem('user_data');
  };

  /**
   * Updates user profile data
   * @param userData Partial user data to update
   */
  const updateUser = (userData: Partial<User>) => {
    if (authState.user) {
      const updatedUser = { ...authState.user, ...userData };
      setAuthState(prev => ({ ...prev, user: updatedUser }));
      localStorage.setItem('user_data', JSON.stringify(updatedUser));
    }
  };

  if (isLoading) {
    return null; // Or a loading spinner
  }

  return (
    <AuthContext.Provider value={{
      ...authState,
      login,
      register,
      logout,
      updateUser
    }}>
      {children}
    </AuthContext.Provider>
  );
};

/**
 * Custom hook to access authentication context
 */
export const useAuth = () => {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};
