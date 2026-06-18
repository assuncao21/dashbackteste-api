import os
from fastapi import APIRouter, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

router = APIRouter()
_security = HTTPBearer(auto_error=False)

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")


def _get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise HTTPException(
            status_code=500,
            detail="SUPABASE_URL e SUPABASE_KEY não configurados no .env",
        )
    from supabase import create_client
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Security(_security),
):
    """Dependência FastAPI: valida o JWT do Supabase e retorna o usuário."""
    if not credentials:
        raise HTTPException(status_code=401, detail="Token não fornecido")
    try:
        client = _get_supabase()
        response = client.auth.get_user(credentials.credentials)
        if not response.user:
            raise HTTPException(status_code=401, detail="Token inválido")
        return response.user
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Token inválido ou expirado")


@router.post("/login")
def login(data: dict):
    email = data.get("email", "").strip()
    password = data.get("password", "")
    if not email or not password:
        raise HTTPException(status_code=400, detail="Email e senha obrigatórios")
    try:
        client = _get_supabase()
        response = client.auth.sign_in_with_password(
            {"email": email, "password": password}
        )
        return {
            "access_token": response.session.access_token,
            "token_type": "bearer",
            "user": {
                "id": str(response.user.id),
                "email": response.user.email,
            },
        }
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Credenciais inválidas")


@router.post("/signup")
def signup(data: dict):
    email = data.get("email", "").strip()
    password = data.get("password", "")
    if not email or not password:
        raise HTTPException(status_code=400, detail="Email e senha obrigatórios")
    if len(password) < 6:
        raise HTTPException(
            status_code=400, detail="Senha deve ter no mínimo 6 caracteres"
        )
    try:
        client = _get_supabase()
        response = client.auth.sign_up({"email": email, "password": password})
        return {
            "message": "Cadastro realizado. Verifique seu email para confirmar a conta.",
            "user": {
                "id": str(response.user.id),
                "email": response.user.email,
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/logout")
def logout(
    credentials: HTTPAuthorizationCredentials = Security(_security),
):
    if not credentials:
        raise HTTPException(status_code=401, detail="Token não fornecido")
    try:
        client = _get_supabase()
        client.auth.sign_out()
        return {"message": "Logout realizado com sucesso"}
    except Exception:
        return {"message": "Logout realizado"}
