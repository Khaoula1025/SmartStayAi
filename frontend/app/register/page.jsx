'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import Link from 'next/link';
import { Loader2, User, Mail, Lock } from 'lucide-react';
import { useAuth } from '../context/AuthContext';
import AlertBanner from '../components/ui/AlertBanner';

export default function RegisterPage() {
  const { user, isLoading } = useAuth();
  const [formData, setFormData] = useState({
    username: '',
    email: '',
    password: '',
    confirmPassword: ''
  });
  const [error, setError] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const router = useRouter();

  useEffect(() => {
    if (!isLoading && user) {
      router.push('/dashboard');
    }
  }, [user, isLoading, router]);

  const handleChange = (e) => {
    setFormData({ ...formData, [e.target.name]: e.target.value });
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');

    // Basic Validation
    if (formData.password !== formData.confirmPassword) {
      setError('Passwords do not match.');
      return;
    }

    setIsSubmitting(true);
    try {
      const { register: apiRegister } = await import('../lib/api');
      await apiRegister(formData.username, formData.email, formData.password);
      
      // Redirect to login with success message
      router.push('/login?registered=true');
    } catch (err) {
      console.error('Registration failed:', err);
      setError(err.message || 'An error occurred during registration. Please try again.');
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="flex min-h-screen bg-surface">
      {/* Left Panel - Reusing branding from login */}
      <div className="hidden lg:flex lg:w-[40%] bg-navy flex-col justify-between p-12 text-white shadow-2xl relative overflow-hidden">
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
            Join the elite revenue management team at UNO Hotels.
          </p>
        </div>
        
        <div className="relative z-10 opacity-70 flex items-center mb-8">
          <div className="h-px bg-white/20 flex-1 mr-4"></div>
          <p className="text-sm font-medium tracking-widest uppercase">Member Registration</p>
          <div className="h-px bg-white/20 flex-1 ml-4"></div>
        </div>
      </div>

      {/* Right Panel - Form */}
      <div className="flex flex-col justify-center px-6 py-12 lg:px-24 flex-1 bg-white relative">
        <div className="sm:mx-auto sm:w-full sm:max-w-md">
          <div className="lg:hidden text-center mb-10">
            <h1 className="text-3xl font-bold tracking-tight text-navy">
              SmartStay <span className="text-gold">Intelligence</span>
            </h1>
          </div>
          
          <h2 className="text-center text-3xl font-bold leading-9 tracking-tight text-text-dark">
            Create an account
          </h2>
          <p className="mt-2 text-center text-sm text-text-muted">
            The Hickstead Hotel, West Sussex
          </p>
        </div>

        <div className="mt-10 sm:mx-auto sm:w-full sm:max-w-md">
          {error && <AlertBanner message={error} type="error" />}
          
          <form className="space-y-5" onSubmit={handleSubmit}>
            <div>
              <label htmlFor="username" className="block text-sm font-medium leading-6 text-text-dark">
                Username
              </label>
              <div className="mt-2 relative">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none text-text-muted">
                  <User className="h-4 w-4" />
                </div>
                <input
                  id="username"
                  name="username"
                  type="text"
                  required
                  value={formData.username}
                  onChange={handleChange}
                  className="block w-full rounded-md border-0 py-2.5 pl-10 pr-3 text-text-dark shadow-sm ring-1 ring-inset ring-border placeholder:text-text-muted focus:ring-2 focus:ring-inset focus:ring-gold sm:text-sm sm:leading-6"
                  placeholder="johndoe"
                />
              </div>
            </div>

            <div>
              <label htmlFor="email" className="block text-sm font-medium leading-6 text-text-dark">
                Email address
              </label>
              <div className="mt-2 relative">
                <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none text-text-muted">
                  <Mail className="h-4 w-4" />
                </div>
                <input
                  id="email"
                  name="email"
                  type="email"
                  required
                  value={formData.email}
                  onChange={handleChange}
                  className="block w-full rounded-md border-0 py-2.5 pl-10 pr-3 text-text-dark shadow-sm ring-1 ring-inset ring-border placeholder:text-text-muted focus:ring-2 focus:ring-inset focus:ring-gold sm:text-sm sm:leading-6"
                  placeholder="john@unohotels.co.uk"
                />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div>
                <label htmlFor="password" className="block text-sm font-medium leading-6 text-text-dark">
                  Password
                </label>
                <div className="mt-2 relative">
                  <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none text-text-muted">
                    <Lock className="h-4 w-4" />
                  </div>
                  <input
                    id="password"
                    name="password"
                    type="password"
                    required
                    value={formData.password}
                    onChange={handleChange}
                    className="block w-full rounded-md border-0 py-2.5 pl-10 pr-3 text-text-dark shadow-sm ring-1 ring-inset ring-border focus:ring-2 focus:ring-inset focus:ring-gold sm:text-sm sm:leading-6"
                  />
                </div>
              </div>

              <div>
                <label htmlFor="confirmPassword" className="block text-sm font-medium leading-6 text-text-dark">
                  Confirm
                </label>
                <div className="mt-2 relative">
                  <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none text-text-muted">
                    <Lock className="h-4 w-4" />
                  </div>
                  <input
                    id="confirmPassword"
                    name="confirmPassword"
                    type="password"
                    required
                    value={formData.confirmPassword}
                    onChange={handleChange}
                    className="block w-full rounded-md border-0 py-2.5 pl-10 pr-3 text-text-dark shadow-sm ring-1 ring-inset ring-border focus:ring-2 focus:ring-inset focus:ring-gold sm:text-sm sm:leading-6"
                  />
                </div>
              </div>
            </div>

            <div>
              <button
                type="submit"
                disabled={isSubmitting}
                className="flex w-full justify-center rounded-md bg-navy px-3 py-2.5 text-sm font-semibold leading-6 text-white shadow-sm hover:bg-gold hover:text-navy focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-navy transition-all duration-200 disabled:opacity-70 disabled:cursor-not-allowed group relative overflow-hidden"
              >
                <span className="absolute inset-0 w-full h-full bg-gradient-to-r from-transparent via-white/10 to-transparent -translate-x-full group-hover:animate-[shimmer_1.5s_infinite]"></span>
                
                {isSubmitting ? (
                  <>
                    <Loader2 className="mr-2 h-5 w-5 animate-spin inline" />
                    Creating account...
                  </>
                ) : (
                  'Sign up'
                )}
              </button>
            </div>
          </form>

          <p className="mt-10 text-center text-sm text-text-muted">
            Already have an account?{' '}
            <Link href="/login" className="font-semibold leading-6 text-navy hover:text-gold transition-colors">
              Sign in here
            </Link>
          </p>
        </div>
      </div>
    </div>
  );
}
