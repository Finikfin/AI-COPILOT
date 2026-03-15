import React, { useState } from 'react';
import { Bell, User, Settings, Menu, X, LogOut } from 'lucide-react';
import { useAuth } from '@/contexts/AuthContext';
import { Button } from '@/components/ui/button';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { Avatar, AvatarFallback } from '@/components/ui/avatar';
import { Badge } from '@/components/ui/badge';
import { useNavigate } from 'react-router-dom';
interface HeaderProps {
  onToggleSidebar: () => void;
}

export const Header: React.FC<HeaderProps> = ({ onToggleSidebar }) => {
  const { user, logout } = useAuth();
  const [notifications] = useState(3);
  const [isProfileOpen, setIsProfileOpen] = useState(false);
  const navigate = useNavigate();

  const handleSettingsClick = () => {
    navigate('/settings'); // Переход на страницу настроек
  };
  const openProfile = () => {
    setIsProfileOpen(true);
  };

  const closeProfile = () => {
    setIsProfileOpen(false);
  };

  return (
    <>
      <header className="h-16 bg-background border-b border-border flex items-center justify-between px-6 sticky top-0 z-50">
        <div className="flex items-center gap-4">
          <Button
            variant="ghost"
            size="sm"
            onClick={onToggleSidebar}
            className="lg:hidden"
          >
            <Menu className="h-5 w-5" />
          </Button>
          
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 bg-primary rounded-lg flex items-center justify-center">
              <span className="text-primary-foreground font-bold text-sm">Ai</span>
            </div>
            <h1 className="text-xl font-semibold text-foreground">AI Copilot</h1>
          </div>
        </div>

        <div className="flex items-center gap-4">
          <Button variant="ghost" size="sm" className="relative">
            <Bell className="h-5 w-5" />
            {notifications > 0 && (
              <Badge className="absolute -top-1 -right-1 px-1 min-w-5 h-5 text-xs bg-red-500">
                {notifications}
              </Badge>
            )}
          </Button>

          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" className="flex items-center gap-2">
                <Avatar className="h-8 w-8">
                  <AvatarFallback className="bg-primary text-primary-foreground">
                    {user?.fullName?.charAt(0).toUpperCase() || 'U'}
                  </AvatarFallback>
                </Avatar>
                <span className="hidden md:block text-sm font-medium">
                  {user?.fullName}
                </span>
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-56">
              <div className="px-2 py-1.5">
                <p className="text-sm font-medium">{user?.fullName}</p>
                <p className="text-xs text-muted-foreground">{user?.email}</p>
                <Badge variant="secondary" className="mt-1 text-xs">
                  {user?.role}
                </Badge>
              </div>
              <DropdownMenuSeparator />
              <DropdownMenuItem 
                className="cursor-pointer"
                onClick={openProfile}
              >
                <User className="mr-2 h-4 w-4" />
                Профиль
              </DropdownMenuItem>
              <DropdownMenuItem 
                className="cursor-pointer"
                onClick={handleSettingsClick}
              >
                <Settings className="mr-2 h-4 w-4" />
                Настройки
              </DropdownMenuItem>
              <DropdownMenuSeparator />
              <DropdownMenuItem 
                className="cursor-pointer text-red-500 focus:text-red-500"
                onClick={logout}
              >
                <LogOut className="mr-2 h-4 w-4" />
                Выйти
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </header>

      {/* Модальное окно профиля */}
      {isProfileOpen && (
        <div className="fixed inset-0 z-50 overflow-y-auto">
          {/* Затемнённый фон */}
          <div 
            className="fixed inset-0 bg-black bg-opacity-50 transition-opacity" 
            onClick={closeProfile}
          />
          
          {/* Само модальное окно */}
          <div className="flex items-center justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
            <div className="inline-block align-bottom bg-background border border-border rounded-lg text-left overflow-hidden shadow-xl transform transition-all sm:my-8 sm:align-middle sm:max-w-lg sm:w-full">
              <div className="bg-background px-4 pt-5 pb-4 sm:p-6 sm:pb-4 text-foreground">
                <div className="flex justify-between items-start">
                  <h3 className="text-lg leading-6 font-medium text-foreground">
                    Профиль пользователя
                  </h3>
                  <button
                    type="button"
                    className="text-muted-foreground hover:text-foreground focus:outline-none"
                    onClick={closeProfile}
                  >
                    <X className="h-6 w-6" />
                  </button>
                </div>
                
                <div className="mt-4">
                  <div className="flex items-center gap-4">
                    <Avatar className="h-16 w-16">
                      <AvatarFallback className="bg-primary text-primary-foreground text-2xl">
                        {user?.fullName?.charAt(0).toUpperCase() || 'U'}
                      </AvatarFallback>
                    </Avatar>
                    <div>
                      <h4 className="text-xl font-semibold">{user?.fullName}</h4>
                      <Badge variant="secondary" className="mt-1">
                        {user?.role}
                      </Badge>
                    </div>
                  </div>
                  
                  <div className="mt-6 space-y-4">
                    <div>
                      <p className="text-sm text-muted-foreground">Email</p>
                      <p className="text-sm font-medium">{user?.email}</p>
                    </div>
                    
                    <div>
                      <p className="text-sm text-muted-foreground">Дата регистрации</p>
                      <p className="text-sm font-medium">
                        {new Date((user as any)?.createdAt || Date.now()).toLocaleDateString()}
                      </p>
                    </div>
                    
                    <div>
                      <p className="text-sm text-muted-foreground">Статус</p>
                      <p className="text-sm font-medium text-primary">
                        Активен
                      </p>
                    </div>
                  </div>
                </div>
              </div>
              
              <div className="bg-muted px-4 py-3 sm:px-6 sm:flex sm:flex-row-reverse border-t border-border">
                <Button
                  type="button"
                  variant="default"
                  className="w-full sm:ml-3 sm:w-auto"
                  onClick={closeProfile}
                >
                  Закрыть
                </Button>
              </div>
            </div>
          </div>
        </div>
      )}
    </>
  );
};

// import React, { useState } from 'react';
// import { Bell, User, Settings, Menu } from 'lucide-react';
// import { useAuth } from '@/contexts/AuthContext';
// import { Button } from '@/components/ui/button';
// import {
//   DropdownMenu,
//   DropdownMenuContent,
//   DropdownMenuItem,
//   DropdownMenuSeparator,
//   DropdownMenuTrigger,
// } from '@/components/ui/dropdown-menu';
// import { Avatar, AvatarFallback } from '@/components/ui/avatar';
// import { Badge } from '@/components/ui/badge';

// interface HeaderProps {
//   onToggleSidebar: () => void;
// }

// export const Header: React.FC<HeaderProps> = ({ onToggleSidebar }) => {
//   const { user, logout } = useAuth();
//   const [notifications] = useState(3);

//   return (
//     <header className="h-16 bg-white border-b border-gray-200 flex items-center justify-between px-6 sticky top-0 z-50">
//       <div className="flex items-center gap-4">
//         <Button
//           variant="ghost"
//           size="sm"
//           onClick={onToggleSidebar}
//           className="lg:hidden"
//         >
//           <Menu className="h-5 w-5" />
//         </Button>
        
//         <div className="flex items-center gap-3">
//           <div className="w-8 h-8 bg-green-600 rounded-lg flex items-center justify-center">
//             <span className="text-white font-bold text-sm">K</span>
//           </div>
//           <h1 className="text-xl font-semibold text-gray-900">KrokOS Graph</h1>
//         </div>
//       </div>

//       <div className="flex items-center gap-4">
//         <Button variant="ghost" size="sm" className="relative">
//           <Bell className="h-5 w-5" />
//           {notifications > 0 && (
//             <Badge className="absolute -top-1 -right-1 px-1 min-w-5 h-5 text-xs bg-red-500">
//               {notifications}
//             </Badge>
//           )}
//         </Button>

//         <DropdownMenu>
//           <DropdownMenuTrigger asChild>
//             <Button variant="ghost" className="flex items-center gap-2">
//               <Avatar className="h-8 w-8">
//                 <AvatarFallback className="bg-green-600 text-white">
//                   {user?.name?.charAt(0).toUpperCase() || 'U'}
//                 </AvatarFallback>
//               </Avatar>
//               <span className="hidden md:block text-sm font-medium">
//                 {user?.name}
//               </span>
//             </Button>
//           </DropdownMenuTrigger>
//           <DropdownMenuContent align="end" className="w-56">
//             <div className="px-2 py-1.5">
//               <p className="text-sm font-medium">{user?.name}</p>
//               <p className="text-xs text-gray-500">{user?.email}</p>
//               <Badge variant="secondary" className="mt-1 text-xs">
//                 {user?.role}
//               </Badge>
//             </div>
//             <DropdownMenuSeparator />
//             <DropdownMenuItem className="cursor-pointer">
//               <User className="mr-2 h-4 w-4" />
//               Профиль
//             </DropdownMenuItem>
//             <DropdownMenuItem className="cursor-pointer">
//               <Settings className="mr-2 h-4 w-4" />
//               Настройки
//             </DropdownMenuItem>
//           </DropdownMenuContent>
//         </DropdownMenu>
//       </div>
//     </header>
//   );
// };
