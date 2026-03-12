'use client';

import { useEffect } from 'react';
import { useRouter, usePathname } from 'next/navigation';
import { useAuth } from '../../context/AuthContext';
import Sidebar from './Sidebar';
import Header from './Header';
import { Loader2 } from 'lucide-react';

export default function DashboardLayout({ children }) {
  const { user, isLoading } = useAuth();
  const router = useRouter();
  const pathname = usePathname();

  useEffect(() => {
    if (!isLoading && !user && !pathname.startsWith('/login')) {
      router.push('/login');
    }
  }, [user, isLoading, router, pathname]);

  if (isLoading) {
    return (
      <div className="min-h-screen bg-navy flex flex-col items-center justify-center text-white">
        <Loader2 className="w-12 h-12 text-gold animate-spin mb-4" />
        <h2 className="text-xl font-semibold">Loading SmartStay Intelligence...</h2>
      </div>
    );
  }

  if (!user && !pathname.startsWith('/login')) {
    return null; // Will redirect in useEffect
  }

  return (
    <div className="flex bg-surface min-h-screen">
      <Sidebar />
      <div className="ml-[240px] flex-1 flex flex-col w-[calc(100%-240px)]">
        <Header />
        <main className="flex-1 p-6 overflow-auto">
          {children}
        </main>
      </div>
    </div>
  );
}
