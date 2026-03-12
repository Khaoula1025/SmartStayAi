'use client';

import { createContext, useContext, useState, useEffect } from 'react';
import { useRouter, usePathname } from 'next/navigation';
import { login as apiLogin, logout as apiLogout, checkSession } from '../lib/api';

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const router = useRouter();
  const pathname = usePathname();

  useEffect(() => {
    verifySession();
  }, []);

  const verifySession = async () => {
    try {
      setIsLoading(true);
      const data = await checkSession();
      if (data && data.message === 'success' && data.user) {
        setUser({ 
          name: data.user.username, 
          role: data.user.role || 'admin',
          email: data.user.email
        });
      } else {
        setUser(null);
      }
    } catch (error) {
      console.error('Session verification failed:', error);
      setUser(null);
    } finally {
      setIsLoading(false);
    }
  };

  const login = async (username, password) => {
    try {
      setIsLoading(true);
      const response = await apiLogin(username, password);
      
      // After login, we should verify the session to get full user details
      await verifySession();
      
      router.push('/dashboard');
      return { success: true };
    } catch (error) {
      console.error('Login error:', error);
      return { success: false, error: typeof error === 'object' ? error.message : 'Invalid username or password' };
    } finally {
      setIsLoading(false);
    }
  };

  const logout = async () => {
    try {
      setIsLoading(true);
      await apiLogout();
    } catch (error) {
      console.error('Logout error:', error);
    } finally {
      setUser(null);
      setIsLoading(false);
      router.push('/login');
    }
  };

  return (
    <AuthContext.Provider value={{ user, login, logout, isLoading }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}
