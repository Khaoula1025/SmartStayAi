'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { useAuth } from '../context/AuthContext';
import { Loader2 } from 'lucide-react';
import AlertBanner from '../components/ui/AlertBanner';

export default function LoginPage() {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  
  const { login, user, isLoading } = useAuth();
  const router = useRouter();

  useEffect(() => {
    if (!isLoading && user) {
      router.push('/dashboard');
    }
  }, [user, isLoading, router]);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    
    if (!username || !password) {
      setError('Please enter both username and password.');
      return;
    }

    setIsSubmitting(true);
    
    try {
      const result = await login(username, password);
      if (!result.success) {
        setError(result.error || 'Login failed. Please check your credentials.');
      }
    } catch (err) {
      setError('An error occurred during login. Please try again.');
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="flex min-h-screen bg-surface">
      {/* Left Panel */}
      <div className="hidden lg:flex lg:w-[40%] bg-navy flex-col justify-between p-12 text-white shadow-2xl relative overflow-hidden">
        {/* Decorative background element */}
        <div className="absolute top-[-10%] left-[-10%] w-[120%] h-[120%] bg-gradient-to-br from-navy-light/40 to-navy-light/0 rounded-full blur-3xl mix-blend-screen pointer-events-none"></div>
        
        <div className="relative z-10">
          <div className="inline-block p-1 bg-white/10 rounded-xl backdrop-blur-sm mb-6">
            <div className="w-12 h-12 bg-white rounded-lg flex items-center justify-center shadow-inner">
              <span className="text-navy font-bold text-xl">SM</span>
            </div>
          </div>
          <h1 className="text-4xl md:text-5xl font-bold tracking-tight mb-2">
            SmartStay
          </h1>
          <h2 className="text-3xl md:text-4xl font-semibold text-gold mb-6">
            Intelligence
          </h2>
          <p className="text-lg text-text-muted max-w-sm mt-8 border-l-4 border-gold pl-4 py-1">
            Advanced revenue management and forecasting for pilot properties.
          </p>
        </div>
        
        <div className="relative z-10 opacity-70 flex items-center mb-8">
          <div className="h-px bg-white/20 flex-1 mr-4"></div>
          <p className="text-sm font-medium tracking-widest uppercase">Powered by UNO Hotels</p>
          <div className="h-px bg-white/20 flex-1 ml-4"></div>
        </div>
      </div>

      {/* Right Panel */}
      <div className="flex flex-col justify-center px-6 py-12 lg:px-24 flex-1 bg-white relative">
        <div className="sm:mx-auto sm:w-full sm:max-w-md">
          {/* Mobile Header (Hidden on large screens) */}
          <div className="lg:hidden text-center mb-10">
            <h1 className="text-3xl font-bold tracking-tight text-navy">
              SmartStay <span className="text-gold">Intelligence</span>
            </h1>
            <p className="mt-2 text-sm text-text-muted">Powered by UNO Hotels</p>
          </div>
          
          <h2 className="text-center text-3xl font-bold leading-9 tracking-tight text-text-dark">
            Sign in to your account
          </h2>
          <p className="mt-2 text-center text-sm text-text-muted">
            The Hickstead Hotel, West Sussex
          </p>
        </div>

        <div className="mt-10 sm:mx-auto sm:w-full sm:max-w-md">
          {error && <AlertBanner message={error} type="error" />}
          {typeof window !== 'undefined' && new URLSearchParams(window.location.search).get('registered') && (
            <AlertBanner 
              message="Account created successfully! Please sign in with your credentials." 
              type="success" 
            />
          )}
          
          <form className="space-y-6" onSubmit={handleSubmit}>
            <div>
              <label 
                htmlFor="username" 
                className="block text-sm font-medium leading-6 text-text-dark"
              >
                Username
              </label>
              <div className="mt-2">
                <input
                  id="username"
                  name="username"
                  type="text"
                  required
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  className="block w-full rounded-md border-0 py-2.5 px-3 text-text-dark shadow-sm ring-1 ring-inset ring-border placeholder:text-text-muted focus:ring-2 focus:ring-inset focus:ring-gold sm:text-sm sm:leading-6 transition-shadow"
                  placeholder="admin"
                />
              </div>
            </div>

            <div>
              <div className="flex items-center justify-between">
                <label 
                  htmlFor="password" 
                  className="block text-sm font-medium leading-6 text-text-dark"
                >
                  Password
                </label>
                <div className="text-sm">
                  <a href="#" className="font-semibold text-navy hover:text-gold transition-colors">
                    Forgot password?
                  </a>
                </div>
              </div>
              <div className="mt-2">
                <input
                  id="password"
                  name="password"
                  type="password"
                  required
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="block w-full rounded-md border-0 py-2.5 px-3 text-text-dark shadow-sm ring-1 ring-inset ring-border placeholder:text-text-muted focus:ring-2 focus:ring-inset focus:ring-gold sm:text-sm sm:leading-6 transition-shadow"
                  placeholder="admin"
                />
              </div>
            </div>

            <div>
              <button
                type="submit"
                disabled={isSubmitting}
                className="flex w-full justify-center rounded-md bg-navy px-3 py-2.5 text-sm font-semibold leading-6 text-white shadow-sm hover:bg-gold hover:text-navy focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-navy transition-all duration-200 disabled:opacity-70 disabled:cursor-not-allowed group relative overflow-hidden"
              >
                {/* Shine effect */}
                <span className="absolute inset-0 w-full h-full bg-gradient-to-r from-transparent via-white/10 to-transparent -translate-x-full group-hover:animate-[shimmer_1.5s_infinite]"></span>
                
                {isSubmitting ? (
                  <>
                    <Loader2 className="mr-2 h-5 w-5 animate-spin inline" />
                    Signing in...
                  </>
                ) : (
                  'Sign in'
                )}
              </button>
            </div>
          </form>

          <p className="mt-10 text-center text-sm text-text-muted">
            Don't have an account?{' '}
            <a href="/register" className="font-semibold leading-6 text-navy hover:text-gold transition-colors">
              Create one now
            </a>
          </p>
        </div>
      </div>
    </div>
  );
}
