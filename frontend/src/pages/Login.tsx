import React, { useState } from "react";
import { useNavigate, Link } from "react-router-dom";
import { useAuth } from "@/contexts/AuthContext";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { toast } from "sonner";
import { LogIn, Loader2 } from "lucide-react";

const Login: React.FC = () => {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const { login, register } = useAuth();
  const { login, register } = useAuth();
  const navigate = useNavigate();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!email || !password) {
      toast.error("Пожалуйста, заполните все поля");
      return;
    }

    setIsLoading(true);
    try {
      // 1. Try to login
      // 1. Try to login
      await login(email, password);
      toast.success("Успешный вход!");
      navigate("/");
    } catch (loginError: any) {
      // 2. If login fails, try to register
      // Note: We only try to register if we suspect the user doesn't exist.
      // Since current backend returns 401 for both wrong password and missing user,
      // we attempt registration. If registration fails with 409, it means the password was wrong.
      try {
        const defaultName = email.split("@")[0];
        await register(email, defaultName, password);
        toast.success("Аккаунт создан и выполнен вход!");
        navigate("/");
      } catch (regError: any) {
        // If registration fails because user exists, then the original 401 was indeed a wrong password
        if (regError.message.includes("уже существует")) {
          toast.error("Неверный пароль для этого аккаунта");
        } else {
          toast.error(regError.message || "Ошибка входа");
        }
      }
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-muted/30 p-4">
      <Card className="w-full max-w-md shadow-lg border-border">
        <CardHeader className="space-y-1 text-center">
          <div className="flex justify-center mb-4">
            <div className="w-12 h-12 bg-primary rounded-xl flex items-center justify-center shadow-lg shadow-primary/20">
              <span className="text-primary-foreground font-bold text-xl">
                Ai
              </span>
            </div>
          </div>
          <CardTitle className="text-2xl font-bold">Вход в систему</CardTitle>
          <CardTitle className="text-2xl font-bold">Вход в систему</CardTitle>
          <CardDescription>
            Введите email и пароль для входа или создания аккаунта Введите email
            и пароль для входа или создания аккаунта
          </CardDescription>
        </CardHeader>
        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="email">Email</Label>
              <Input
                id="email"
                type="email"
                placeholder="email@example.com"
                placeholder="email@example.com"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                disabled={isLoading}
                required
                className="bg-background border-border"
              />
            </div>
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <Label htmlFor="password">Пароль</Label>
              </div>
              <Input
                id="password"
                type="password"
                placeholder="••••••••"
                placeholder="••••••••"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                disabled={isLoading}
                required
                className="bg-background border-border"
              />
              <p className="text-[10px] text-muted-foreground mt-1">
                Для новых пользователей: минимум 8 символов, буквы и цифры.
              </p>
              <p className="text-[10px] text-muted-foreground mt-1">
                Для новых пользователей: минимум 8 символов, буквы и цифры.
              </p>
            </div>
            <Button
              type="submit"
              className="w-full h-11 gap-2 mt-2"
              disabled={isLoading}
            >
              {isLoading ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <LogIn className="h-4 w-4" />
              )}
              {isLoading ? "Обработка..." : "Войти"}
            </Button>
          </form>
        </CardContent>
        <CardFooter className="flex flex-col space-y-4">
          <div className="text-center text-xs text-muted-foreground">
            Если у вас нет аккаунта, он будет создан автоматически при первом
            входе.
          </div>
        </CardFooter>
      </Card>
    </div>
  );
};

export default Login;
