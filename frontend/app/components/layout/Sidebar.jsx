'use client';

import { usePathname, useRouter } from 'next/navigation';
import Link from 'next/link';
import { LayoutDashboard, TrendingUp, DollarSign, BarChart2, Activity, LogOut } from 'lucide-react';
import { useAuth } from '../../context/AuthContext';

const navItems = [
  { name: 'Dashboard', path: '/dashboard', icon: LayoutDashboard },
  { name: 'Forecast', path: '/forecast', icon: TrendingUp },
  { name: 'Rate Decisions', path: '/rates', icon: DollarSign },
  { name: 'Analytics', path: '/analytics', icon: BarChart2 },
  { name: 'Pipeline', path: '/pipeline', icon: Activity },
];

export default function Sidebar() {
  const pathname = usePathname();
  const { user, logout } = useAuth();
  const router = useRouter();

  const handleLogout = async () => {
    await logout();
  };

  const getInitials = (name) => {
    if (!name) return 'U';
    return name.substring(0, 2).toUpperCase();
  };

  return (
    <div className="w-[240px] bg-navy text-white h-screen fixed top-0 left-0 flex flex-col pt-6 pb-6 shadow-xl z-20">
      <div className="px-6 mb-8">
        <h1 className="text-2xl font-bold tracking-tight">
          SmartStay <span className="text-gold font-semibold">Intelligence</span>
        </h1>
        <p className="text-text-muted text-sm mt-1">The Hickstead Hotel</p>
      </div>

      <nav className="flex-1 px-3 space-y-1">
        {navItems.map((item) => {
          const isActive = pathname.startsWith(item.path);
          const Icon = item.icon;

          return (
            <Link
              key={item.name}
              href={item.path}
              className={`flex items-center px-3 py-3 rounded-md transition-colors ${
                isActive
                  ? 'bg-navy-light text-gold border-l-4 border-gold'
                  : 'text-gray-300 hover:bg-navy-light hover:text-white border-l-4 border-transparent'
              }`}
            >
              <Icon className="w-5 h-5 mr-3" />
              <span className="font-medium">{item.name}</span>
            </Link>
          );
        })}
      </nav>

      <div className="px-6 mt-auto">
        <div className="flex items-center pt-6 border-t border-navy-light">
          <div className="w-10 h-10 rounded-full bg-gold-light text-navy flex items-center justify-center font-bold text-lg">
            {getInitials(user?.name)}
          </div>
          <div className="ml-3 flex-1 overflow-hidden">
            <p className="text-sm font-medium text-white truncate">{user?.name || 'Loading...'}</p>
            <p className="text-xs text-text-muted capitalize">{user?.role || 'User'}</p>
          </div>
          <button
            onClick={handleLogout}
            className="p-2 text-text-muted hover:text-white transition-colors ml-1 rounded-md hover:bg-navy-light"
            title="Sign out"
          >
            <LogOut className="w-5 h-5" />
          </button>
        </div>
      </div>
    </div>
  );
}
