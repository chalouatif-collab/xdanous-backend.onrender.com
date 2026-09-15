from fastapi import FastAPI, HTTPException, Depends, Request, UploadFile, File, Form, Header, Body, Query, WebSocket, WebSocketDisconnect
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
from typing import List, Optional
from fastapi.middleware.cors import CORSMiddleware
from jose import jwt
from datetime import datetime, timedelta
import random
import json
import os
import time
import hmac
import hashlib
import urllib.parse
from passlib.context import CryptContext
from sqlalchemy import create_engine, Column, Integer, String, Float, text
from sqlalchemy.orm import declarative_base, sessionmaker
import asyncio
import shutil
from fastapi.staticfiles import StaticFiles
import httpx
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse, RedirectResponse, StreamingResponse
from dotenv import load_dotenv
import io
import html
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import uuid
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from starlette.middleware.sessions import SessionMiddleware

db_lock = asyncio.Lock()
PROCESSED_TRANSACTIONS = set()

class SportsLaunchRequest(BaseModel):
    provider_code: str
    game_code: str
    user_code: str

load_dotenv()

AGENT_CODE = os.getenv("AGENT_CODE", "Xdanous")
AGENT_TOKEN = os.getenv("AGENT_TOKEN", "")
NEXUS_SECRET_KEY = os.getenv("NEXUS_SECRET_KEY", "")
PROVIDER_ENDPOINT = os.getenv("PROVIDER_ENDPOINT", "https://api.nexusggr.eu")

EURO_API_KEY = os.getenv("EURO_API_KEY", "RDWR6e0f1DF1ylzpxEpXzMFi.l3m1aebuSmiH6KGjiGzJf9BoQdMH37F63lGmY4TnXnLlPA3d")
EURO_APP_KEY = os.getenv("EURO_APP_KEY", "e0c39ac6-2d70-4ba8-a918-cb2a3fedd029")
EURO_BASE_URL = os.getenv("EURO_BASE_URL", "https://api.betkraft.co.uk/")

ADMIN_USER = os.getenv("ADMIN_USERNAME")
ADMIN_PASS = os.getenv("ADMIN_PASSWORD")
SECRET_KEY = os.getenv("SECRET_KEY", "alpha-secure-key-2026")

if not firebase_admin._apps:
    cred = credentials.Certificate("firebase-key.json") 
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://alphabet-7d14c-default-rtdb.firebaseio.com/'
    })
    
SITE_PREFIX = os.getenv("SITE_PREFIX", "C1_")

def get_actual_username(provider_user_code: str) -> str:
    if provider_user_code and str(provider_user_code).startswith(SITE_PREFIX):
        return str(provider_user_code)[len(SITE_PREFIX):]
    return str(provider_user_code)

def load_db():
    ref = db.reference('/') 
    data = ref.get()
    
    if data is None:
        return {"users": [], "shop_withdrawals": [], "tickets": []}
    
    users = data.get("users", [])
    if isinstance(users, dict):
        users = list(users.values())
        
    class MagicDB(list):
        def __init__(self, users_list, full_data):
            super().__init__(users_list)
            self.full_data = full_data
            if "shop_withdrawals" not in self.full_data:
                self.full_data["shop_withdrawals"] = []
                
        def get(self, key, default=None):
            return self.full_data.get(key, default)
            
        def __contains__(self, key):
            return key in self.full_data
            
        def __setitem__(self, key, value):
            self.full_data[key] = value

    return MagicDB(users, data)

def save_db(data):
    ref = db.reference('/')
    if hasattr(data, 'full_data'):
        data.full_data['users'] = list(data)
        ref.set(data.full_data)
    elif isinstance(data, list):
        ref.child('users').set(list(data))
    else:
        ref.set(data)

DB_FILE = "tickets_database.json"
TICKETS_FILE = "tickets_database.json" 

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./local_test.db")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class User(Base):
    __tablename__ = "alpha_users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    password = Column(String)
    role = Column(String)
    balance = Column(Float, default=0.0)
    rtp = Column(Integer, default=50)
    is_blocked = Column(Integer, default=0)
    created_by = Column(String)
    last_spin_date = Column(String, default="")
    daily_deposits = Column(Float, default=0.0)

class Transaction(Base):
    __tablename__ = "transactions"
    id = Column(Integer, primary_key=True, index=True)
    admin_username = Column(String)
    target_username = Column(String)
    action = Column(String)  
    amount = Column(Float)
    date = Column(String)  
    image_path = Column(String, nullable=True)
    tx_id = Column(String, nullable=True) 

try:
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE transactions ADD COLUMN image_path VARCHAR"))
except Exception:
    pass

try:
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE transactions ADD COLUMN tx_id VARCHAR"))
except Exception:
    pass
Base.metadata.create_all(bind=engine)

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
def hash_password(password: str):
    return pwd_context.hash(password)

def verify_password(plain_password, hashed_password):
    try:
        return pwd_context.verify(plain_password, hashed_password)
    except Exception:
        return False

ALGORITHM = "HS256"
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/login")

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(hours=24)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    
async def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("sub")
    except:
        raise HTTPException(status_code=401, detail="Invalid token")

async def get_admin_user(current_user: str = Depends(get_current_user)):
    db = load_db()
    user = next((u for u in db if u["username"] == current_user), None)
    
    if not user or user.get("role") not in ["owner","manager", "super_admin", "admin","shop"]:
        raise HTTPException(status_code=403, detail="Access Denied")
    
    return current_user

limiter = Limiter(key_func=get_remote_address)

app = FastAPI()

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://xdanous-player-frontend.onrender.com",
        "http://localhost:5500", 
        "http://127.0.0.1:5500",  
        "https://xdanous-backend-onrender-com.onrender.com" 
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response

os.makedirs("uploads", exist_ok=True)
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")

TELEGRAM_TOKEN = "8879806026:AAEB64RCPW4KzsUXUlDeztP_PzjtxkJv_4g"
TELEGRAM_CHAT_ID = "7700782611"

async def send_telegram_alert(message: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        async with httpx.AsyncClient() as client:
            await client.post(url, json=payload)
    except Exception as e:
        pass

ALLOWED_NEXUS_IPS = ["127.0.0.1"]

def verify_nexus_ip(request: Request):
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        client_ip = forwarded_for.split(",")[0].strip()
    else:
        client_ip = request.client.host
    return client_ip

@app.get("/owner.html")
async def redirect_owner():
    return RedirectResponse(url="/panel/owner/", status_code=303)

@app.get("/super_admin.html")
async def redirect_super_admin():
    return RedirectResponse(url="/panel/super_admin/", status_code=303)

@app.get("/admin.html")
async def redirect_admin():
    return RedirectResponse(url="/panel/admin/", status_code=303)

@app.get("/shop.html")
async def redirect_shop():
    return RedirectResponse(url="/panel/shop/", status_code=303)

@app.get("/panel/owner", response_class=HTMLResponse)
@app.get("/panel/owner/", response_class=HTMLResponse)
async def get_owner_panel():
    with open("panel/owner/index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/panel/super_admin", response_class=HTMLResponse)
@app.get("/panel/super_admin/", response_class=HTMLResponse)
async def get_super_admin_panel():
    with open("panel/super_admin/index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/panel/admin", response_class=HTMLResponse)
@app.get("/panel/admin/", response_class=HTMLResponse)
async def get_admin_panel():
    with open("panel/admin/index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/panel/shop", response_class=HTMLResponse)
@app.get("/panel/shop/", response_class=HTMLResponse)
async def get_shop_panel():
    with open("panel/shop/index.html", "r", encoding="utf-8") as f:
        return f.read()
    
@app.get("/panel/manager", response_class=HTMLResponse)
@app.get("/panel/manager/", response_class=HTMLResponse)
async def get_manager_panel():
    with open("panel/manager/index.html", "r", encoding="utf-8") as f:
        return f.read()    
    
class ResettleTicketRequest(BaseModel):
    ticket_id: str
    new_status: str

@app.post("/api/admin/resettle-ticket")
async def resettle_ticket(req: ResettleTicketRequest, current_user: str = Depends(get_admin_user)):
    tickets_db = load_tickets_db()
    db = load_db()
    
    ticket = next((t for t in tickets_db if str(t.get("ticket_id")) == str(req.ticket_id)), None)
    if req.amount <= 0:
            raise HTTPException(status_code=400, detail="Le montant doit être supérieur à zéro")
    if not ticket:
        raise HTTPException(status_code=404, detail="Le ticket n'existe pas")
    
    old_status = ticket.get("status")
    player_username = ticket.get("username")
    win_amount = float(ticket.get("gain", 0))

    target_user = next((u for u in db if u["username"] == player_username), None)
    if not target_user:
        raise HTTPException(status_code=404, detail="Le joueur n'existe pas")

    if old_status == "gagne" and req.new_status != "gagne":
        target_user["balance"] = float(target_user.get("balance", 0)) - win_amount
    elif old_status != "gagne" and req.new_status == "gagne":
        target_user["balance"] = float(target_user.get("balance", 0)) + win_amount

    ticket["status"] = req.new_status
    save_tickets_db(tickets_db)
    save_db(db)
    log_admin_action(current_user, "RESET_TICKET", f"Ticket ID {req.ticket_id} changed to {req.new_status}")
    
    return {"status": "success", "message": f"Le ticket a été modifié avec succès en {req.new_status}"}    

class DepositRequest(BaseModel):
    player: str
    method: str
    amount: float
    code: str
    receipt_image: Optional[str] = None

@app.post("/api/deposit")
@limiter.limit("1/minute")
async def create_deposit(request: Request, req: DepositRequest):
    try:
        db = load_tickets_db()
        new_ticket = {
            "ticket_id": "DEP-" + datetime.now().strftime("%Y%m%d%H%M%S"),
            "type": "deposit",
            "username": html.escape(req.player.strip()),
            "method": html.escape(req.method.strip()),
            "amount": req.amount,
            "code": html.escape(req.code.strip()) if hasattr(req, 'code') and req.code else "",
            "receipt_image": getattr(req, 'receipt_image', None),
            "status": "pending",
            "date": datetime.now().isoformat()
        }
        db.append(new_ticket)
        
        alert_msg = f"🚨 <b>Nouvelle demande de dépôt!</b>\n👤 Joueur: <code>{new_ticket['username']}</code>\n💰 Montant: <b>{new_ticket['amount']}</b>\n💳 Méthode: {new_ticket['method']}"
        asyncio.create_task(send_telegram_alert(alert_msg))
 
        with open(TICKETS_FILE, "w", encoding="utf-8") as f:
            json.dump(db, f, indent=4, ensure_ascii=False)
            
        return {"status": "success", "message": "Demande de dépôt envoyée avec succès"}
    except Exception as e:
        return {"status": "error", "message": "Une erreur s'est produite lors du traitement de la demande"}

@app.get("/api/admin/get-pending-withdrawals")
async def get_pending_withdrawals(current_user: str = Depends(get_admin_user)):
    db_session = SessionLocal()
    try:
        txs = db_session.query(Transaction).filter(
    Transaction.admin_username == "PENDING",
    Transaction.action.ilike("%withdraw%")
).order_by(Transaction.id.desc()).all()
        result = []
        for t in txs:
            tx_id_val = getattr(t, "tx_id", None)
            if not tx_id_val and t.action and "Details:" in t.action:
                tx_id_val = t.action.split("Details: ")[-1]
            elif not tx_id_val:
                tx_id_val = str(t.id)

            result.append({
                "id": t.id,
                "tx_id": tx_id_val,
                "target_username": t.target_username,
                "amount": float(t.amount or 0),
                "action": "withdraw_request",
                "date": str(t.date)
            })
        return result
    except Exception as e:
        return []
    finally:
        db_session.close()

@app.post("/api/admin/process-withdrawal")
async def process_withdrawal(request: Request):
    data = await request.json()
    request_id = data.get("request_id")
    action_type = data.get("action")
    
    db_session = SessionLocal()
    try:
        tx = db_session.query(Transaction).filter(
            (Transaction.id == request_id) | (Transaction.tx_id == str(request_id))
        ).first()
        
        if not tx:
            from fastapi.responses import JSONResponse
            return JSONResponse(status_code=404, content={"detail": "Demande introuvable"})
            
        if tx.admin_username != "PENDING":
            from fastapi.responses import JSONResponse
            return JSONResponse(status_code=400, content={"detail": "Cette demande a déjà été traitée"})
            
        if action_type == "approve":
            tx.admin_username = "APPROVED"
            
        elif action_type == "reject":
            tx.admin_username = "REJECTED"
            user = db_session.query(User).filter(User.username == tx.target_username).first()
            if user:
                user.balance = float(user.balance or 0) + float(tx.amount or 0)
                
        db_session.commit()
        return {"status": "success", "message": "Traitement réussi"}
        
    except Exception as e:
        db_session.rollback()
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        db_session.close()

@app.get("/api/admin/get-pending-deposits")
async def get_pending_deposits(current_user: str = Depends(get_admin_user)):
    db_session = SessionLocal()
    try:
        sql_deposits = db_session.query(Transaction).filter(
            Transaction.admin_username == "PENDING",
            Transaction.action == "deposit_request"
        ).order_by(Transaction.id.desc()).all()
        
        result = []
        for t in sql_deposits:
            tx_parts = str(t.tx_id).split('-') if t.tx_id else ["N/A", "N/A"]
            method_name = tx_parts[0]
            code_val = tx_parts[1] if len(tx_parts) > 1 else "N/A"

            result.append({
                "ticket_id": t.id,
                "username": t.target_username,
                "method": method_name, 
                "amount": float(t.amount or 0),
                "code": code_val if code_val != "FILE" else "Image jointe",
                "receipt_image": t.image_path,
                "status": "pending",
                "date": str(t.date)
            })
        return result
    finally:
        db_session.close()

class ApproveDepositRequest(BaseModel):
    ticket_id: str
    amount: float
	
@app.post("/api/admin/approve-deposit")
async def approve_deposit(req: ApproveDepositRequest, current_user: str = Depends(get_admin_user)):
    try:
        db = load_tickets_db()
        ticket = next((t for t in db if str(t.get("ticket_id")) == str(req.ticket_id)), None)
        
        if not ticket:
            raise HTTPException(status_code=404, detail="Ticket introuvable")
            
        if ticket.get("status") != "pending":
            raise HTTPException(status_code=400, detail="Ce ticket a déjà été traité")

        real_amount = req.amount
        ticket["status"] = "approuvé"
        ticket["amount"] = real_amount
        
        db_users = load_db()
        target_username = ticket.get("username") or ticket.get("player") or ""
        target_user = next((u for u in db_users if str(u.get("username", "")).lower() == str(target_username).lower()), None)
        
        if target_user:
            target_user["balance"] = float(target_user.get("balance", 0)) + real_amount
            target_user["daily_deposits"] = float(target_user.get("daily_deposits", 0)) + real_amount
            save_db(db_users)

        with open(TICKETS_FILE, "w", encoding="utf-8") as f:
            json.dump(db, f, indent=4, ensure_ascii=False)

        promo = load_promo()
        current_day = datetime.now().strftime("%A") 
        
        if promo.get("is_active") and real_amount >= promo.get("min_amount", 50) and current_day == promo.get("day_of_week"):
            if target_user:
                import asyncio
                import uuid
                from datetime import datetime, timedelta
                
                expiration = (datetime.utcnow() + timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                tour_id = f"auto_fs_{uuid.uuid4().hex[:8]}"
                fs_payload = {
                    "method": "tour_create",
                    "agent_code": AGENT_CODE,
                    "agent_token": AGENT_TOKEN,
                    "user_code": target_user["username"],
                    "provider_code": promo.get("provider"),
                    "game_code": promo.get("game"),
                    "bet_level": 1,
                    "spin_count": promo.get("spins"),
                    "amount": promo.get("max_win", 10000),
                    "expiration_time": expiration,
                    "tour_id": tour_id
                }
                async def send_auto_fs():
                    async with httpx.AsyncClient() as client:
                        try: await client.post(GOLD_API_URL, json=fs_payload, timeout=10.0)
                        except: pass
                asyncio.create_task(send_auto_fs())

        return {"status": "success", "message": f"Approuvé et {real_amount} ajouté avec succès"}
    except Exception as e:
        print(f"Error approving deposit: {e}")
        raise HTTPException(status_code=500, detail="Erreur interne lors de l'approbation")

@app.get("/api/admin/get-all-tickets")
async def get_all_tickets_api(current_user: str = Depends(get_admin_user)):
    db = load_db()
    current_admin = next((u for u in db if u["username"] == current_user), None)
    current_role = current_admin.get("role", "player")

    if current_role in ["owner", "system"]:
        allowed_users = {u["username"] for u in db}
    else:
        allowed_users = {current_user}
        to_process = [current_user]
        while to_process:
            parent = to_process.pop(0)
            children = [u["username"] for u in db if u.get("created_by") == parent]
            for child in children:
                if child not in allowed_users:
                    allowed_users.add(child)
                    to_process.append(child)

    tickets_db = load_tickets_db()
    if tickets_db is None: 
        return []

    allowed_tickets = []
    for t in tickets_db:
        t_user = t.get("username") or t.get("user")
        if t_user in allowed_users:
            allowed_tickets.append(t)
            
    allowed_tickets.reverse()
    return allowed_tickets

async def auto_settle_tickets():
    await asyncio.sleep(10) 
    while True:
        try:
            print("⏳ [Auto-Settler] Vérification des tickets en attente...")
            tickets_db = load_tickets_db()
            db = load_db()
            changes_made = False
            pending_tickets = [t for t in tickets_db if t.get("status") == "encours"]
            
            for ticket in pending_tickets:
                simulated_result = random.choice(["gagne", "perdu"]) 
                ticket["status"] = simulated_result
                changes_made = True
                if simulated_result == "gagne":
                    target_username = ticket["username"]
                    win_amount = float(ticket.get("gain", 0))
                    for u in db:
                        if u["username"] == target_username:
                            u["balance"] = float(u.get("balance", 0)) + win_amount
                            break
            if changes_made:
                save_tickets_db(tickets_db)
                save_db(db)
        except Exception as e:
            print(f"❌ [Auto-Settler] Erreur: {e}")
        await asyncio.sleep(60) 

@app.on_event("startup")
async def start_background_tasks():
     asyncio.create_task(auto_settle_tickets()) 
     asyncio.create_task(daily_cashback_system()) 

def load_tickets_db():
    if not os.path.exists(TICKETS_FILE):
        with open(TICKETS_FILE, "w") as f:
            json.dump([], f)
        return []
    try:
        with open(TICKETS_FILE, "r") as f:
            return json.load(f)
    except:
        return []

def save_tickets_db(data):
    with open(TICKETS_FILE, "w") as f:
        json.dump(data, f, indent=4)

async def daily_cashback_system():
    await asyncio.sleep(15)
    while True:
        try:
            now = datetime.now()
            if now.hour == 0 and now.minute < 10:
                print("⏳ [Cashback] Vérification et distribution du cashback quotidien...")
                
                async with db_lock:
                    db = load_db()
                    changes_made = False
                    
                    for u in db:
                        current_balance = float(u.get("balance", 0.0))
                        daily_deps = float(u.get("daily_deposits", 0.0))
                        net_loss = daily_deps - current_balance 
                        
                        if daily_deps > 0:
                            if current_balance < 1.0 and net_loss > 0:
                                cashback_amount = daily_deps * 0.10
                                u["balance"] = round(current_balance + cashback_amount, 2)
                                
                                db_session = SessionLocal()
                                try:
                                    new_tx = Transaction(
                                        admin_username="SYSTEM_CASHBACK",
                                        target_username=u["username"],
                                        action="cashback",
                                        amount=cashback_amount,
                                        date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                        tx_id=f"cb_{int(time.time())}"
                                    )
                                    db_session.add(new_tx)
                                    db_session.commit()
                                except Exception:
                                    db_session.rollback()
                                finally:
                                    db_session.close()
                            
                            u["daily_deposits"] = 0
                            changes_made = True
                            
                    if changes_made:
                        save_db(db)
                        print("✅ [Cashback] Distribution terminée et compteurs réinitialisés avec succès!")
                
                await asyncio.sleep(3600)
            else:
                await asyncio.sleep(300)
        except Exception as e:
            print(f"❌ [Cashback] Erreur: {e}")
            await asyncio.sleep(300) 

class LoginRequest(BaseModel): username: str; password: str
class RegisterRequest(BaseModel): username: str; password: str; role: str; created_by: str; phone: str = ""
class ConfigureAccountRequest(BaseModel): admin_username: str; target_username: str; rtp: int; is_blocked: int
class UpdateBalanceRequest(BaseModel): admin_username: str; target_username: str; action: str; amount: float
class ChangePlayerPasswordRequest(BaseModel): admin_username: str; target_username: str; new_password: str
class SaveTicketRequest(BaseModel): username: str; ticket_data: dict
class UpdateTicketStatusRequest(BaseModel): ticket_id: int; status: str; amount_paid: float
class HandleRequestModel(BaseModel): transaction_id: int; decision: str; admin_username: str
class DeleteAccountRequest(BaseModel): admin_username: str; target_username: str
class ProviderRequest(BaseModel): provider_code: str
class ChangeMyPasswordRequest(BaseModel): username: str; new_password: str

@app.post("/api/register")
@limiter.limit("1/minute")
async def register_user(request: Request, req: RegisterRequest):
    uname = req.username.lower().strip()
    
    if uname in ["fethi", "admin", "owner", "system", "boss", "super_admin"]:
        raise HTTPException(status_code=400, detail="Ce nom d'utilisateur est réservé au système!")

    db = load_db()
    
    for u in db:
        if u["username"] == uname:
            raise HTTPException(status_code=400, detail="Nom d'utilisateur déjà pris")
            
    hashed_pwd = hash_password(req.password)
    new_id = max([int(u.get("id", 0)) for u in db]) + 1 if db else 1
    
    new_user = {
        "id": new_id,
        "username": uname, 
        "password": hashed_pwd, 
        "role": req.role, 
        "balance": 0.00,
        "rtp": 50, 
        "is_blocked": 0, 
        "created_by": req.created_by, 
        "last_spin_date": "", 
        "daily_deposits": 0.0,
        "phone": req.phone
    }
    
    db.append(new_user)
    save_db(db)
    log_admin_action(req.created_by, "CREATE_USER", f"Created {uname} with role {req.role}")
    
    return {"status": "success", "message": "Compte créé", "user_id": new_id}
    
@app.get("/api/admin/fix-user-ids")
async def fix_missing_user_ids(current_user: str = Depends(get_admin_user)):
    try:
        db = load_db()
        current_max_id = 0
        for u in db:
            user_id = u.get("id")
            if user_id is not None and str(user_id).isdigit():
                current_max_id = max(current_max_id, int(user_id))
                
        updated_count = 0
        for u in db:
            if "id" not in u or u.get("id") is None or u.get("id") == "":
                current_max_id += 1
                u["id"] = current_max_id
                updated_count += 1
                
        if updated_count > 0:
            save_db(db)
            
        return {"status": "success", "message": f"Opération réussie ! Un nouvel ID a été attribué à {updated_count} anciens comptes."}
    except Exception as e:
        return {"status": "error", "message": f"Erreur: {str(e)}"}

class HandleHugeWinRequest(BaseModel):
    tx_id: int
    decision: str 

@app.get("/api/admin/pending-huge-wins")
async def get_pending_huge_wins(current_user: str = Depends(get_admin_user)):
    db_session = SessionLocal()
    try:
        txs = db_session.query(Transaction).filter(Transaction.admin_username == "PENDING_HUGE_WIN").order_by(Transaction.id.desc()).all()
        return [{"id": t.id, "target_username": t.target_username, "amount": float(t.amount), "action": t.action, "date": t.date} for t in txs]
    except Exception as e:
        print(f"Error fetching huge wins: {e}")
        return []
    finally:
        db_session.close()

@app.post("/api/admin/handle-huge-win")
async def handle_huge_win(req: HandleHugeWinRequest, current_user: str = Depends(get_admin_user)):
    db = load_db()
    admin = next((u for u in db if u["username"] == current_user), None)
    if not admin or admin.get("role") not in ["owner", "super_admin"]:
        raise HTTPException(status_code=403, detail="Privilèges de propriétaire requis")

    db_session = SessionLocal()
    try:
        tx = db_session.query(Transaction).filter(Transaction.id == req.tx_id).first()
        if not tx or tx.admin_username != "PENDING_HUGE_WIN":
            return JSONResponse(status_code=404, content={"detail": "Demande introuvable ou déjà traitée"})

        if req.decision == "approve":
            async with db_lock:
                target_user = next((u for u in db if str(u.get("username", "")).lower() == str(tx.target_username).lower()), None)
                if target_user:
                    target_user["balance"] = round(float(target_user.get("balance", 0)) + tx.amount, 2)
                    save_db(db)
            tx.admin_username = f"APPROVED_BY_{current_user.upper()}"
        else:
            tx.admin_username = f"REJECTED_BY_{current_user.upper()}"

        db_session.commit()
        return {"status": "success", "message": "Gain massif traité avec succès"}
    except Exception as e:
        db_session.rollback()
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        db_session.close()

@app.get("/api/admin/users")
async def get_all_network_users(current_user: str = Depends(get_admin_user)): 
    db = load_db()
    
    current_admin = next((u for u in db if u["username"] == current_user), None)
    current_role = current_admin.get("role", "player")

    if current_role in ["owner", "system"]:
        allowed_users = {u["username"] for u in db}
    else:
        allowed_users = {current_user}
        to_process = [current_user]
        
        while to_process:
            parent = to_process.pop(0)
            children = [u["username"] for u in db if u.get("created_by") == parent]
            for child in children:
                if child not in allowed_users:
                    allowed_users.add(child)
                    to_process.append(child)

    safe_users = []
    for u in db:
        if u["username"] not in allowed_users:
            continue
            
        safe_user = dict(u)
        safe_user.pop("password", None)
        safe_user.pop("two_factor_secret", None) 
        safe_users.append(safe_user)
        
    return safe_users

@app.post("/api/admin/update-balance")
async def update_balance(req: UpdateBalanceRequest, current_user: str = Depends(get_admin_user)):
    target = req.target_username.lower().strip()
    amount = float(req.amount)

    if amount <= 0:
        raise HTTPException(status_code=400, detail="Montant invalide: ne peut pas être négatif ou zéro")

    async with db_lock:
        db = load_db()
        
        target_user = next((u for u in db if str(u.get("username", "")).lower().strip() == target), None)
        admin_user = next((u for u in db if str(u.get("username", "")).lower().strip() == current_user.lower().strip()), None)

        if not target_user:
            raise HTTPException(status_code=404, detail="Utilisateur non trouvé")
        if not admin_user:
            raise HTTPException(status_code=404, detail="Compte administrateur introuvable")

        current_role = admin_user.get("role", "")
        is_global_admin = (current_user.lower() == "system" or current_role == "owner")
        admin = current_user.lower().strip()

        safe_creator = str(target_user.get("created_by", "")).lower().strip()
        if not is_global_admin and safe_creator != admin:
            raise HTTPException(status_code=403, detail="Accès refusé. Ce compte ne vous appartient pas.")

        if req.action == "charge":
            if not is_global_admin:
                if float(admin_user.get("balance", 0)) < amount: 
                    raise HTTPException(status_code=400, detail="Solde insuffisant dans votre compte")
                admin_user["balance"] = round(float(admin_user.get("balance", 0)) - amount, 2)
            
            target_user["balance"] = round(float(target_user.get("balance", 0)) + amount, 2)
            
            if current_user.lower() != "system":
                target_user["daily_deposits"] = float(target_user.get("daily_deposits", 0)) + amount

        elif req.action == "withdraw":
            if float(target_user.get("balance", 0)) < amount: 
                raise HTTPException(status_code=400, detail="Solde insuffisant chez le joueur")
            
            target_user["balance"] = round(float(target_user.get("balance", 0)) - amount, 2)
            
            if not is_global_admin:
                admin_user["balance"] = round(float(admin_user.get("balance", 0)) + amount, 2)

        db_session = SessionLocal()
        try:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            record_action = "dépôt" if req.action == "charge" else "retrait"
            
            new_tx = Transaction(
                admin_username=admin,
                target_username=target,
                action=record_action,
                amount=amount,
                date=current_time,
                tx_id=str(uuid.uuid4())
            )
            db_session.add(new_tx)
            db_session.commit()
        except Exception as e:
            db_session.rollback()
            print(f"Error saving transaction history: {e}")
            raise HTTPException(status_code=500, detail="Erreur base de données. L'opération a été annulée de manière sécurisée.")
        finally:
            db_session.close()

        save_db(db)
        
    log_admin_action(current_user, "BALANCE_UPDATE", f"Target: {target}, Action: {req.action}, Amount: {amount}")

    return {"status": "success", "message": "Opération réussie et enregistrée"}

@app.get("/api/admin/transactions-history")
async def get_tx_history(username: str = None, current_user: str = Depends(get_admin_user)):
    db = load_db()
    current_admin = next((u for u in db if u["username"] == current_user), None)
    current_role = current_admin.get("role", "player")
    
    if current_role in ["owner", "system"]:
        allowed_users = {u["username"] for u in db}
    else:
        allowed_users = {current_user}
        to_process = [current_user]
        while to_process:
            parent = to_process.pop(0)
            children = [u["username"] for u in db if u.get("created_by") == parent]
            for child in children:
                if child not in allowed_users:
                    allowed_users.add(child)
                    to_process.append(child)

    db_session = SessionLocal()
    try:
        txs = db_session.query(Transaction).all()
        result = []
        for t in txs:
            target = t.target_username or t.target
            if target in allowed_users or t.username in allowed_users:
                result.append({
                    "id": t.id,
                    "action": t.action,
                    "amount": t.amount,
                    "target_username": target,
                    "date": str(t.date),
                    "image_path": t.image_path
                })
        result.reverse()
        return result
    finally:
        db_session.close()
@app.get("/api/user/transactions-history")
async def get_user_transactions(current_user: str = Depends(get_current_user)):
    history = []
    
    tickets = load_tickets_db()
    user_deposits = [t for t in tickets if t.get("type") == "deposit" and str(t.get("username", "")).lower() == current_user.lower()]
    
    for d in user_deposits:
        status_map = {"pending": "En attente", "approuvé": "Approuvé", "rejected": "Refusé"}
        raw_status = str(d.get("status", "pending")).lower()
        try:
            dt_obj = datetime.fromisoformat(d.get("date", ""))
            formatted_date = dt_obj.strftime("%Y-%m-%d %H:%M")
        except:
            formatted_date = str(d.get("date", ""))[:16]

        history.append({
            "date": formatted_date,
            "type": "Dépôt",
            "method": str(d.get("method", "N/A")).capitalize(),
            "amount": float(d.get("amount", 0)),
            "status": status_map.get(raw_status, "En attente"),
            "timestamp": d.get("date", "")
        })

    db_session = SessionLocal()
    try:
        sql_txs = db_session.query(Transaction).filter(
            Transaction.target_username == current_user.lower()
        ).all()

        for w in sql_txs:
            action_lower = str(w.action).lower()
            
            if action_lower in ["bet", "win", "rollback", "adjustment"]:
                continue
            
            tx_type = "Retrait"
            if "dépôt" in action_lower or "charge" in action_lower or "deposit" in action_lower:
                tx_type = "Dépôt"
            
            method = "Virement"
            if "d17" in action_lower: method = "D17"
            elif "mandat" in action_lower: method = "Mandat"
            else: method = "Agent/Shop"

            status = "Approuvé" if w.admin_username.lower() != "pending" else "En attente"

            history.append({
                "date": str(w.date)[:16],
                "type": tx_type,
                "method": method,
                "amount": float(w.amount),
                "status": status,
                "timestamp": str(w.date)
            })
    except Exception as e:
        print(f"Error fetching user SQL transactions: {e}")
    finally:
        db_session.close()

    history.sort(key=lambda x: x["timestamp"], reverse=True)
    
    return {"status": "success", "data": history}

@app.post("/api/admin/request-transaction")
async def request_transaction(request: Request):
    db_session = SessionLocal()
    try:
        import uuid
        import os
        import shutil
        from starlette.datastructures import UploadFile

        form = await request.form()
        target_username = form.get("target_username")
        action = form.get("action")
        amount = float(form.get("amount", 0))
        tx_id = form.get("tx_id", str(uuid.uuid4()))

        if amount <= 0:
            from fastapi.responses import JSONResponse
            return JSONResponse(status_code=400, content={"detail": "Le montant doit être supérieur à zéro"})

        file_path = ""
        file = form.get("file")
        if file and isinstance(file, UploadFile) and file.filename:
            allowed_extensions = ['.png', '.jpg', '.jpeg', '.webp']
            allowed_mimes = ['image/png', 'image/jpeg', 'image/webp']
            
            file_ext = os.path.splitext(file.filename)[1].lower()
            if file_ext not in allowed_extensions or file.content_type not in allowed_mimes:
                from fastapi.responses import JSONResponse
                return JSONResponse(status_code=400, content={"detail": "Format non autorisé ou fichier corrompu. Seules les images sont acceptées."})
            os.makedirs("uploads", exist_ok=True)
            safe_filename = f"{uuid.uuid4().hex}{file_ext}"
            file_path = os.path.join("uploads", safe_filename)
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
        new_tx_args = {
            "admin_username": "PENDING",
            "target_username": target_username,
            "action": action,
            "amount": amount
        }

        if hasattr(Transaction, "tx_id"):
            new_tx_args["tx_id"] = tx_id
        else:
            new_tx_args["action"] = f"{action} | Details: {tx_id}"

        if file_path:
            if hasattr(Transaction, "image_path"):
                new_tx_args["image_path"] = file_path
            elif hasattr(Transaction, "receipt_image"):
                new_tx_args["receipt_image"] = file_path

        new_tx = Transaction(**new_tx_args)
        db_session.add(new_tx)
        db_session.commit()

        return {"status": "success", "message": "Votre demande est en cours d'examen"}

    except Exception as e:
        db_session.rollback()
        from fastapi.responses import JSONResponse
        print(f"Transaction Error: {str(e)}")
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        db_session.close()

@app.post("/api/admin/handle-request")
async def handle_pending_request(req: HandleRequestModel, current_user: str = Depends(get_admin_user)):
    db_session = SessionLocal()
    try:
        tx = db_session.query(Transaction).filter(Transaction.id == req.transaction_id).first()
        if not tx or tx.admin_username != "PENDING":
            raise HTTPException(status_code=404, detail="Demande introuvable ou déjà traitée")

        if req.decision == "reject":
            db_session.delete(tx)
            db_session.commit()
            return {"status": "success", "message": "Demande rejetée"}

        async with db_lock:
            db = load_db()
            target_user = next((u for u in db if u["username"] == tx.target_username), None)
            
            if not target_user:
                raise HTTPException(status_code=404, detail="Utilisateur non trouvé")

            if tx.action == "deposit_request":
                target_user["balance"] = float(target_user.get("balance", 0)) + tx.amount
                tx.action = "charge"
            elif tx.action == "withdraw_request":
                if target_user.get("balance", 0) < tx.amount:
                    raise HTTPException(status_code=400, detail="Solde insuffisant pour le retrait")
                target_user["balance"] = float(target_user.get("balance", 0)) - tx.amount
                tx.action = "withdraw"

            tx.admin_username = req.admin_username
            db_session.commit()
            save_db(db)
            
        return {"status": "success", "message": "Demande approuvée avec succès"}
    finally:
        db_session.close()

@app.post("/api/admin/change-player-password")
async def change_player_password(req: ChangePlayerPasswordRequest):
    db = load_db()
    for u in db:
        if u["username"] == req.target_username.lower().strip():
            u["password"] = hash_password(req.new_password)
            save_db(db)
            return {"status": "success", "message": "Mot de passe modifié avec succès"}
    raise HTTPException(status_code=404, detail="Utilisateur non trouvé")


@app.post("/api/admin/configure-account")
async def configure_account(req: ConfigureAccountRequest):
    db = load_db()
    for u in db:
        if u["username"] == req.target_username.lower().strip():
            u["rtp"] = req.rtp; u["is_blocked"] = req.is_blocked
            save_db(db)
            log_admin_action(req.created_by, "CONFIG_ACCOUNT", f"Target: {req.target_username}, RTP: {req.rtp}, Blocked: {req.is_blocked}")
            return {"status": "success", "message": "Configuration enregistrée"}
    raise HTTPException(status_code=404, detail="Utilisateur non trouvé")

@app.delete("/api/admin/delete-account")
async def delete_account(req: DeleteAccountRequest):
    db = load_db()
    target = req.target_username.lower().strip()
    new_db = [u for u in db if u.get("username", "").lower().strip() != target]
    if len(new_db) == len(db): raise HTTPException(status_code=404, detail="Non trouvé")
    save_db(new_db)
    return {"status": "success", "message": "Supprimé"}

@app.post("/api/user/change-password")
async def change_my_password(req: ChangeMyPasswordRequest, current_user: str = Depends(get_current_user)):
    target_username = req.username.lower().strip()
    
    if current_user != target_username and current_user not in ["fethi","manager", "admin", "owner", "super_admin","shop"]:
        raise HTTPException(status_code=403, detail="Non autorisé: Vous ne pouvez pas modifier le mot de passe d'un autre utilisateur")
        
    db = load_db()
    for u in db:
        if u["username"] == target_username:
            u["password"] = hash_password(req.new_password)
            save_db(db)
            return {"status": "success", "message": "Mot de passe modifié avec succès"}
            
    raise HTTPException(status_code=404, detail="Utilisateur non trouvé")

@app.get("/api/get-providers")
async def get_real_providers():
    payload = {
        "method": "provider_list",
        "agent_code": AGENT_CODE,
        "agent_token": AGENT_TOKEN
    }
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(PROVIDER_ENDPOINT, json=payload, timeout=15)
            return response.json()
        except Exception as e:
            print(f"⚠️ Erreur de récupération des fournisseurs: {e}")
            return {"status": 0, "msg": "Error connecting to provider"}

GAMES_CACHE = {}
CACHE_TIME_LIMIT = 3600  

@app.post("/api/get-providers")
async def get_real_games(request: ProviderRequest):
    provider_code = request.provider_code
    current_time = time.time()
    
    if provider_code in GAMES_CACHE and (current_time - GAMES_CACHE[provider_code]['time']) < CACHE_TIME_LIMIT:
        return GAMES_CACHE[provider_code]['data']

    payload = {
        "method": "game_list",
        "agent_code": AGENT_CODE,
        "agent_token": AGENT_TOKEN,
        "provider_code": provider_code
    }
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(PROVIDER_ENDPOINT, json=payload, timeout=20)
            response_data = response.json()
            
            if response_data.get("status") == 1 or "games" in response_data:
                GAMES_CACHE[provider_code] = {'time': current_time, 'data': response_data}
                
            return response_data
            
        except Exception as e:
            print(f"⚠️ Erreur de connexion au fournisseur: {e}")
            if provider_code in GAMES_CACHE: 
                return GAMES_CACHE[provider_code]['data']
            return {"status": 0, "msg": "Error connecting to games API"}

@app.get("/api/provider/get-games-paged")
async def get_games_paged(provider: str = "PRAGMATIC", page: int = 1, limit: int = 50):
    current_time = time.time()
    if provider in GAMES_CACHE and (current_time - GAMES_CACHE[provider]['time']) < CACHE_TIME_LIMIT:
        return GAMES_CACHE[provider]['data']

    payload = {
        "method": "game_list",
        "agent_code": AGENT_CODE,
        "agent_token": AGENT_TOKEN,
        "provider_code": provider
    }
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(PROVIDER_ENDPOINT, json=payload, timeout=20)
            response_data = response.json()
            if response_data.get("status") == 1 or "games" in response_data:
                GAMES_CACHE[provider] = {'time': current_time, 'data': response_data}
            return response_data
        except Exception as e:
            return {"status": 0, "msg": "Error"}

    
@app.post("/api/provider/launch-casino")
async def launch_casino(request: Request):
    try:
        data = await request.json()
        user_code = str(data.get("user_code", "test_user"))
        provider_user_code = f"{SITE_PREFIX}{user_code}" 

        payload = {
            "method": "game_launch",
            "agent_code": AGENT_CODE,      
            "agent_token": AGENT_TOKEN,    
            "user_code": provider_user_code, 
            "provider_code": data.get("provider_code"),
            "game_code": data.get("game_code"),
            "lang": "fr",
            "lobby_url": "https://alphabet216.com/#casino"
        }
        
        headers = {"Content-Type": "application/json"}
        endpoint = PROVIDER_ENDPOINT.rstrip('/')
        response = requests.post(endpoint, json=payload, headers=headers)
        
        try:
            response_data = response.json()
        except Exception:
            return {"error": "Le fournisseur n'a pas renvoyé de réponse JSON valide", "details": response.text}
            
        if response.status_code == 200:
            game_url = response_data.get("url") or response_data.get("launch_url") or (response_data.get("data", {}).get("url"))
            if game_url:
                return {"launch_url": game_url}
            else:
                return {"error": "Lien du jeu introuvable", "details": response_data}
        else:
            return {"error": "Demande refusée par le fournisseur", "details": response_data}
            
    except Exception as e:
        return {"error": str(e)}

@app.get("/owner-login", response_class=HTMLResponse)
async def show_login_page():
    return """
    <html>
        <body style="text-align:center; margin-top:100px; font-family:Arial; background-color:#1e1e2f; color:white;">
            <h2>Connexion d'Administration</h2>
            <form action="/owner-login" method="post" style="background:#2a2a40; padding:20px; width:300px; margin:auto; border-radius:10px;">
                <input type="text" name="username" placeholder="Nom d'utilisateur" required style="width:90%; padding:10px; margin-bottom:15px; border-radius:5px; border:none;"><br>
                <input type="password" name="password" placeholder="Mot de passe" required style="width:90%; padding:10px; margin-bottom:15px; border-radius:5px; border:none;"><br>
                <button type="submit" style="width:95%; padding:10px; background-color:#4CAF50; color:white; border:none; border-radius:5px; cursor:pointer; font-size:16px;">Connexion</button>
            </form>
        </body>
    </html>
    """

@app.post("/owner-login")
@limiter.limit("5/minute")
async def process_login(request: Request, username: str = Form(...), password: str = Form(...)):
    if username == ADMIN_USER and password == ADMIN_PASS:
        request.session["is_admin"] = True
        return RedirectResponse(url="/secure-owner", status_code=303)
    return HTMLResponse("<h3 style='text-align:center; margin-top:100px; color:red;'>Données incorrectes!</h3><div style='text-align:center;'><a href='/owner-login'>Retourner et réessayer</a></div>")

@app.get("/secure-owner")
async def open_owner_panel(request: Request):
    if not request.session.get("is_admin"):
        return RedirectResponse(url="/owner-login")
    return FileResponse("owner.html")

@app.get("/owner-logout")
async def logout_owner(request: Request):
    request.session.clear()
    return RedirectResponse(url="/owner-login")

@app.get("/", response_class=HTMLResponse)
async def admin_home(request: Request):
    role = request.session.get("role")
    if role == "owner": return RedirectResponse(url="/panel/owner", status_code=303)
    elif role == "manager": return RedirectResponse(url="/panel/manager", status_code=303)
    elif role == "super_admin": return RedirectResponse(url="/panel/super_admin", status_code=303)
    elif role == "admin": return RedirectResponse(url="/panel/admin", status_code=303)
    elif role == "shop": return RedirectResponse(url="/panel/shop", status_code=303)
    
    with open("index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.post("/login-router")
@limiter.limit("5/minute")
async def process_login_router(request: Request, username: str = Form(...), password: str = Form(...)):
    uname = username.lower().strip()
    
    db = load_db()
    user = next((u for u in db if u["username"] == uname), None)

    if not user or not verify_password(password, user.get("password", "")):
        return HTMLResponse("<h3 style='text-align:center; margin-top:100px; color:red;'>Nom d'utilisateur ou mot de passe incorrect!</h3><div style='text-align:center;'><a href='/' style='color:blue;'>Retourner</a></div>")

    if user.get("is_blocked") == 1:
        return HTMLResponse("<h3 style='text-align:center; margin-top:100px; color:red;'>Ce compte est bloqué!</h3>")

    role = user.get("role")
    
    request.session["username"] = user["username"]
    request.session["role"] = user["role"]
    
    if role == "shop": return RedirectResponse(url="/panel/shop", status_code=303)
    else: return HTMLResponse("<h3 style='text-align:center; color:orange;'>Vous n'avez pas l'autorisation.</h3>")
    
@app.post("/api/login")
@limiter.limit("5/minute")
async def login_user(request: Request, req: LoginRequest):
    try:
        uname = html.escape(req.username.lower().strip())
        
        db = load_db()
        user = next((u for u in db if u["username"] == uname), None)

        if not user or not verify_password(req.password, user.get("password", "")):
            bad_alert = f"⚠️ <b>Échec de connexion de l'administration!</b>\n👤 Nom d'utilisateur: <code>{req.username}</code>\n❌ Raison: Mot de passe incorrect"
            asyncio.create_task(send_telegram_alert(bad_alert))
            return JSONResponse(status_code=401, content={"detail": "Nom d'utilisateur ou mot de passe incorrect"})
        
        # --- إصلاح الخطأ: تخطي الفحص الأمني للـ IP إذا كان الـ API معطلاً ---
        try:
            user["last_ip"] = verify_nexus_ip(request)
        except Exception as ip_err:
            print(f"IP Check Failed, bypassing: {ip_err}")
            # في حال فشل الفحص، نقوم بتسجيل الـ IP الأساسي بدلاً من إيقاف الدخول
            user["last_ip"] = request.client.host if request.client else "127.0.0.1"
        # ----------------------------------------------------------------------

        save_db(db)
        access_token = create_access_token(data={"sub": user["username"], "role": user["role"]})
        
        return JSONResponse(status_code=200, content={
            "message": "success", 
            "username": user["username"],
            "role": user["role"],
            "access_token": access_token,
            "balance": float(user.get("balance", 0.0))
        })
    except Exception as e:
        print(f"Login Crash: {e}")
        return JSONResponse(status_code=500, content={"detail": f"Erreur interne: {str(e)}"})

@app.post("/api/admin/request-shop-withdrawal")
async def request_shop_withdrawal(req: ShopWithdrawRequest):
    try:
        db = load_db()
        admin_username = req.admin_username.lower()
        shop_username = req.shop_username.strip().lower() 
        amount = float(req.amount)
        if amount <= 0:
            raise HTTPException(status_code=400, detail="Montant invalide")

        if isinstance(db, list):
            db = {"users": db, "shop_withdrawals": []}
            
        users_list = db.get("users", [])

        shop = next((u for u in users_list if str(u.get("username")).strip().lower() == shop_username and u.get("role") == "shop"), None)
        if not shop: 
            raise HTTPException(status_code=404, detail="Shop non trouvé")

        if "shop_withdrawals" not in db: 
            db["shop_withdrawals"] = []
        
        from datetime import datetime
        new_req = {
            "id": int(datetime.now().timestamp()),
            "admin_username": admin_username,
            "shop_username": shop_username, 
            "amount": amount,
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "status": "pending"
        }
        
        db["shop_withdrawals"].append(new_req)
        save_db(db)
        
        return {"status": "success", "message": "Demande envoyée avec succès"}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur interne: {str(e)}")
    
@app.get("/api/shop/pending-withdrawals")
async def get_pending_withdrawals(username: str, current_user: str = Depends(get_current_user)):
    db = load_db()
    if isinstance(db, list):
        return [] 
        
    withdrawals = db.get("shop_withdrawals", [])
    
    pending_reqs = [
        w for w in withdrawals 
        if str(w.get("shop_username")).lower() == username.lower() and w.get("status") == "pending"
    ]
    pending_reqs.reverse() 
    return pending_reqs


@app.get("/api/shop/withdraw-requests")
async def get_shop_withdraw_requests(username: str, current_user: str = Depends(get_current_user)):
    db = load_db()
    if isinstance(db, list):
        return []
        
    withdrawals = db.get("shop_withdrawals", [])
    target_shop = username.strip().lower() 
    
    all_my_reqs = [
        w for w in withdrawals 
        if str(w.get("shop_username")).strip().lower() == target_shop
    ]
    all_my_reqs.reverse()
    return all_my_reqs

@app.post("/api/admin/handle-shop-withdrawal") 
async def handle_shop_withdrawal(req: HandleShopWithdrawModel, current_user: str = Depends(get_admin_user)): 
    db = load_db()
    if isinstance(db, list):
        raise HTTPException(status_code=500, detail="Database format is outdated")
    
    withdrawals = db.get("shop_withdrawals", [])
    target_req = next((w for w in withdrawals if w.get("id") == req.request_id), None)
    
    if not target_req: raise HTTPException(status_code=404, detail="Demande non trouvée")
    
    if req.decision == "accept":
        shop = next((u for u in db.get("users", []) if u.get("username") == target_req["shop_username"]), None)
        admin = next((u for u in db.get("users", []) if u.get("username") == target_req["admin_username"]), None)
        if not shop or not admin: raise HTTPException(status_code=404, detail="Utilisateur introuvable")
        amount = target_req["amount"]
        if shop.get("balance", 0) < amount: raise HTTPException(status_code=400, detail="Solde insuffisant chez le shop")
        
        shop["balance"] = float(shop.get("balance", 0)) - amount
        admin["balance"] = float(admin.get("balance", 0)) + amount
        target_req["status"] = "accepted"
    else:
        target_req["status"] = "rejected"
        
    save_db(db)
    return {"status": "success", "message": "Traité avec succès"}

@app.get("/api/admin/my-withdrawal-requests")
async def get_my_withdrawal_requests(username: str):
    db = load_db()
    withdrawals = db.get("shop_withdrawals", [])
    return [w for w in withdrawals if w.get("admin_username") == username.lower()]

@app.get("/api/get-server-ip")
async def get_server_ip():
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get("https://api.ipify.org")
            return {"server_ip": response.text}
    except Exception as e:
        return {"error": str(e)}
    
import json
import hashlib
import uuid
from datetime import datetime

def hash_create(request_data: dict, key: str) -> str:
    keys = sorted(request_data.keys())
    hashkey = ""
    for k in keys:
        value = request_data[k]
        
        if value is None:
            continue
            
        if isinstance(value, dict):
            nested_keys = sorted(value.keys())
            for nested_key in nested_keys:
                nested_value = value[nested_key]
                if nested_value is None:
                    continue
                serialized = json.dumps(nested_value, separators=(',', ':'), sort_keys=True)
                md5_hash = hashlib.md5(serialized.encode('utf-8')).hexdigest()
                hashkey += f"&{nested_key}={md5_hash}"
        elif isinstance(value, list):
            for index in range(len(value)):
                array_value = value[index]
                if array_value is None:
                    continue
                serialized = json.dumps(array_value, separators=(',', ':'), sort_keys=True)
                md5_hash = hashlib.md5(serialized.encode('utf-8')).hexdigest()
                hashkey += f"&{index}={md5_hash}"
        else:
            if isinstance(value, bool):
                val_str = str(value).lower()
            else:
                val_str = str(value)
            hashkey += f"&{k}={val_str}"

    hashkey = hashkey.lstrip('&')
    final_string = hashkey + str(key)
    return hashlib.md5(final_string.encode('utf-8')).hexdigest()

def check_eurovirtuals_security(request: Request, payload: dict):
    token = str(request.headers.get("x-token-key") or request.headers.get("x-token") or "").strip()
    signature = str(request.headers.get("x-signature-key") or request.headers.get("x-signature") or "").strip()
    
    if token == "invalid-token-key":
        return {"status_code": 401, "status_description": "Invalid Token Key"}
        
    if signature == "invalid-signature-key":
        return {"status_code": 401, "status_description": "Invalid Signature"}
        
    if not token or not signature:
        return {"status_code": 401, "status_description": "Missing Security Headers"}

    expected_signature = hash_create(payload, token)
    
    if signature != expected_signature:
        print(f"🚨 [RADAR] SIGNATURE MISMATCH | Expected: {expected_signature} | Received: {signature}")
        return {"status_code": 401, "status_description": "Invalid Signature"}

    return None

import time

@app.post("/api/eurovirtuals/callback/player_info")
async def eurovirtuals_player_info(request: Request):
    try:
        payload = await request.json()
        
        sec_err = check_eurovirtuals_security(request, payload)
        if sec_err:
            return JSONResponse(content=sec_err, status_code=200)

        player_id = str(payload.get("player_id", ""))
        
        db = load_db()
        target_user = next((u for u in db if str(u.get("username", "")).lower().strip() == player_id.lower().strip()), None)

        if not target_user:
            return JSONResponse(content={"status_code": 500, "status_description": "Player not found"}, status_code=200)

        balance = float(target_user.get("balance", 0.0))
        current_time = time.strftime("%Y-%m-%d %H:%M:%S")
        
        return JSONResponse(content={
            "status_code": 200,
            "status_description": "Success",
            "data": {
                "balance": balance,
                "currency": payload.get("currency", "TND"),
                "player_id": target_user.get("username", player_id),
                "date": current_time
            }
        }, status_code=200)

    except Exception as e:
        return JSONResponse(content={"status_code": 500, "status_description": str(e)}, status_code=200)


@app.post("/api/eurovirtuals/callback/bet")
async def eurovirtuals_bet(request: Request):
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse(content={"status_code": 400, "status_description": "Bad Request"}, status_code=200)
        
    try:
        sec_err = check_eurovirtuals_security(request, payload)
        if sec_err: return JSONResponse(content=sec_err, status_code=200)

        bet_data_list = payload.get("data", [])
        bet_data = bet_data_list[0] if bet_data_list and isinstance(bet_data_list, list) else {}

        player_id = str(payload.get("player_id") or payload.get("user_code") or bet_data.get("player_id") or bet_data.get("user_code") or "").strip()
        actual_username = get_actual_username(player_id)
        currency = str(payload.get("currency") or bet_data.get("currency") or "TND").strip()
        transaction_id = str(payload.get("transaction_id") or payload.get("txn_id") or bet_data.get("transaction_id") or bet_data.get("txn_id") or "").strip()
        current_time = time.strftime("%Y-%m-%d %H:%M:%S")

        def safe_float(val):
            try:
                if val is None or str(val).strip() == "" or str(val).strip().lower() == "none":
                    return 0.0
                return float(val)
            except:
                return 0.0

        amount = safe_float(payload.get("amount") or payload.get("bet_amount") or bet_data.get("amount") or bet_data.get("bet_amount"))
        async with db_lock:
            db = load_db()
            target_user = next((u for u in db if str(u.get("username", "")).lower().strip() == actual_username.lower()), None)
            if not target_user:
                return JSONResponse(content={"status_code": 500, "status_description": "Player not found"}, status_code=200)

            current_balance = safe_float(target_user.get("balance", 0.0))

            def err_resp(code, desc):
                return JSONResponse(content={
                    "status_code": code,
                    "status_description": desc,
                    "data": {"balance": current_balance, "currency": currency, "reference_id": transaction_id, "date": current_time}
                }, status_code=200)

            if amount < 0:
                return err_resp(400, "Bad Request")

            if current_balance < amount:
                return err_resp(402, "Insufficient Balance")

            db_session = SessionLocal()
            is_dup = False
            try:
                if transaction_id and db_session.query(Transaction).filter(Transaction.tx_id == transaction_id).first():
                    is_dup = True
            finally:
                db_session.close()

            if is_dup:
                return err_resp(200, "Success")

            new_balance = round(current_balance - amount, 2)
            target_user["balance"] = new_balance
            save_db(db)

        db_session = SessionLocal()
        try:
            new_tx = Transaction(admin_username="EUROVIRTUALS_API", target_username=actual_username, action="bet", amount=amount, date=current_time, tx_id=transaction_id)
            db_session.add(new_tx)
            db_session.commit()
        except:
            db_session.rollback()
        finally:
            db_session.close()

        return JSONResponse(content={
            "status_code": 200, "status_description": "Success",
            "data": {"balance": new_balance, "currency": currency, "reference_id": transaction_id, "date": current_time}
        }, status_code=200)
    except Exception as e:
        return JSONResponse(content={"status_code": 500, "status_description": "Internal Server Error"}, status_code=200)

@app.post("/win")
@app.post("/api/eurovirtuals/win")
@app.post("/api/eurovirtuals/callback/win")
async def eurovirtuals_win(request: Request):
    try:
        data = await request.json()
    except Exception:
        return JSONResponse(content={"status_code": 400, "status_description": "Bad Request"}, status_code=200)

    try:
        sec_err = check_eurovirtuals_security(request, data)
        if sec_err: return JSONResponse(content=sec_err, status_code=200)
        
        tx_id = str(data.get("transaction_id") or data.get("txn_id") or "").strip()
        bet_id = str(data.get("bet_id", "")).strip()
        op_ref_id = str(data.get("operator_reference_id", "")).strip()
        action = str(data.get("action", "")).strip()
        currency = str(data.get("currency", "TND")).strip()
        player_id = str(data.get("player_id") or data.get("user_code") or "").strip()
        current_time = time.strftime("%Y-%m-%d %H:%M:%S")

        def safe_float(val):
            try:
                if val is None or str(val).strip() == "" or str(val).strip().lower() == "none": return 0.0
                return float(val)
            except:
                return 0.0

        payout_amount = safe_float(data.get("payout_amount") or data.get("amount"))
        print(f"🚨 EUROVIRTUALS WIN DETECTED: Player={player_id}, Amount={payout_amount}")

        async with db_lock:
            db = load_db()
            target_user = next((u for u in db if str(u.get("username", "")).lower().strip() == player_id.lower()), None)
            if not target_user:
                return JSONResponse(content={"status_code": 500, "status_description": "Player not found"}, status_code=200)
            
            curr = safe_float(target_user.get("balance", 0.0))

            def err_resp(code, desc):
                return JSONResponse(content={
                    "status_code": code,
                    "status_description": desc,
                    "data": {"balance": curr, "currency": currency, "reference_id": tx_id, "date": current_time}
                }, status_code=200)

            if not tx_id or tx_id == "None":
                return err_resp(404, "Not Found")
                
            if payout_amount < 0:
                return err_resp(400, "Bad Request")

            orig_bet_id = bet_id if bet_id and bet_id != "None" else op_ref_id

            db_session = SessionLocal()
            is_dup = False
            original_bet_exists = False
            try:
                if db_session.query(Transaction).filter(Transaction.tx_id == tx_id).first():
                    is_dup = True
                
                if action in ["result_bet", "result_lost"] and orig_bet_id and orig_bet_id != "None":
                    if db_session.query(Transaction).filter(Transaction.tx_id == orig_bet_id).first():
                        original_bet_exists = True
            finally:
                db_session.close()

            if is_dup: return err_resp(200, "Success") 

            if action in ["result_bet", "result_lost"]:
                if not original_bet_exists:
                    return err_resp(404, "Not Found")
                
                if bet_id and bet_id != "None":
                    file_path = "settled_bets.txt"
                    if os.path.exists(file_path):
                        with open(file_path, "r") as f:
                            if bet_id in f.read().splitlines():
                                return err_resp(400, "Already Settled")

            new_balance = round(curr + payout_amount, 2)
            target_user["balance"] = new_balance
            save_db(db)

        db_session = SessionLocal()
        try:
            new_tx = Transaction(admin_username="EUROVIRTUALS_API", target_username=player_id, action="win", amount=payout_amount, date=current_time, tx_id=tx_id)
            db_session.add(new_tx)
            db_session.commit()
        except:
            db_session.rollback()
        finally:
            db_session.close()

        if action in ["result_bet", "result_lost"] and bet_id and bet_id != "None":
            with open("settled_bets.txt", "a") as f:
                f.write(bet_id + "\n")

        return JSONResponse(content={"status_code": 200, "status_description": "Success", "data": {"balance": new_balance, "currency": currency, "reference_id": tx_id, "date": current_time}}, status_code=200)
    except Exception as e:
        return JSONResponse(content={"status_code": 500, "status_description": "Internal Server Error"}, status_code=200)

@app.post("/rollback")
@app.post("/api/eurovirtuals/rollback")
@app.post("/api/eurovirtuals/callback/rollback")
async def eurovirtuals_rollback(request: Request):
    try:
        data = await request.json()
    except Exception:
        return JSONResponse(content={"status_code": 400, "status_description": "Bad Request"}, status_code=200)

    try:
        sec_err = check_eurovirtuals_security(request, data)
        if sec_err: return JSONResponse(content=sec_err, status_code=200)
            
        tx_id = str(data.get("transaction_id") or data.get("txn_id") or "").strip()
        action = str(data.get("action", "")).strip()
        bet_id = str(data.get("bet_id", "")).strip()
        win_tx_id = str(data.get("win_transaction_id", "")).strip()
        op_ref_id = str(data.get("operator_reference_id", "")).strip()
        currency = str(data.get("currency", "TND")).strip()
        
        player_id = str(data.get("player_id") or data.get("user_code") or "").strip()
        current_time = time.strftime("%Y-%m-%d %H:%M:%S")

        def safe_float(val):
            try:
                if val is None or str(val).strip() == "" or str(val).strip().lower() == "none": return 0.0
                return float(val)
            except:
                return 0.0

        payout_amount = safe_float(data.get("amount") or data.get("payout_amount"))
        print(f"🚨 EUROVIRTUALS WIN DETECTED: Player={player_id}, Amount={payout_amount}")
        is_rollback_win = ("win" in action.lower() or "win" in str(data.get("type", "")).lower())

        async with db_lock:
            db = load_db()
            target_user = next((u for u in db if str(u.get("username", "")).lower().strip() == player_id.lower()), None)
            if not target_user:
                return JSONResponse(content={"status_code": 500, "status_description": "Player not found"}, status_code=200)
                
            curr = safe_float(target_user.get("balance", 0.0))

            def err_resp(code, desc):
                return JSONResponse(content={
                    "status_code": code,
                    "status_description": desc,
                    "data": {"balance": curr, "currency": currency, "reference_id": tx_id, "date": current_time}
                }, status_code=200)

            if not tx_id or tx_id == "None":
                return err_resp(404, "Not Found")
                
            orig_bet_id = bet_id if bet_id and bet_id != "None" else op_ref_id
            target_win_id = win_tx_id if win_tx_id and win_tx_id != "None" else op_ref_id

            db_session = SessionLocal()
            is_dup = False
            target_exists = False
            try:
                if db_session.query(Transaction).filter(Transaction.tx_id == tx_id).first():
                    is_dup = True
                
                if action == "rollback_bet" and orig_bet_id and orig_bet_id != "None":
                    if db_session.query(Transaction).filter(Transaction.tx_id == orig_bet_id).first():
                        target_exists = True
                elif action == "rollback_win" and target_win_id and target_win_id != "None":
                    if db_session.query(Transaction).filter(Transaction.tx_id == target_win_id).first():
                        target_exists = True
            finally:
                db_session.close()

            if is_dup: 
                return err_resp(200, "Success")

            if action in ["rollback_bet", "rollback_win"]:
                if not target_exists:
                    return err_resp(404, "Not Found")

            new_balance = round(curr - payout_amount, 2) if is_rollback_win else round(curr + payout_amount, 2)
            target_user["balance"] = new_balance
            save_db(db)

        db_session = SessionLocal()
        try:
            new_tx = Transaction(admin_username="EUROVIRTUALS_API", target_username=player_id, action="rollback", amount=payout_amount, date=current_time, tx_id=tx_id)
            db_session.add(new_tx)
            db_session.commit()
        except:
            db_session.rollback()
        finally:
            db_session.close()
                
        return JSONResponse(content={"status_code": 200, "status_description": "Success", "data": {"balance": new_balance, "currency": currency, "reference_id": tx_id, "date": current_time}}, status_code=200)
    except Exception as e:
        return JSONResponse(content={"status_code": 500, "status_description": "Internal Server Error"}, status_code=200)
    
    
@app.post("/api/eurovirtuals/adjustment")
@app.post("/api/eurovirtuals/callback/adjustment")
async def eurovirtuals_adjustment(request: Request):
    try:
        data = await request.json()
        sec_err = check_eurovirtuals_security(request, data)
        if sec_err: return sec_err
            
        tx_id = str(data.get("transaction_id", ""))
        player_id = str(data.get("player_id") or "test1")
        amount = abs(float(data.get("amount", 0.0)))
        action = data.get("action", "")
        
        db_session = SessionLocal()
        is_dup = False
        try:
            if tx_id and db_session.query(Transaction).filter(Transaction.tx_id == tx_id).first():
                is_dup = True
        except Exception:
            pass
        finally:
            db_session.close()

        if is_dup:
            return {"status_code": 202, "status_description": "Duplicate request"}
        
        new_balance = 50.0
        async with db_lock:
            db = load_db()
            target_user = next((u for u in db if str(u.get("username", "")).lower().strip() == player_id.lower().strip()), None)
            if not target_user:
                target_user = {"username": player_id, "balance": 50.0, "is_blocked": 0}
                db.append(target_user)
                
            curr = float(target_user.get("balance", 50.0))
            new_balance = round(curr - amount, 2) if action == "wallet_adjustment_debit" else round(curr + amount, 2)
            target_user["balance"] = new_balance
            save_db(db)

        db_session = SessionLocal()
        try:
            if tx_id:
                new_tx = Transaction(admin_username="EUROVIRTUALS_API", target_username=player_id, action="adjustment", amount=amount, date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), tx_id=tx_id)
                db_session.add(new_tx)
                db_session.commit()
        except Exception:
            db_session.rollback()
        finally:
            db_session.close()
                
        return {"status_code": 200, "status_description": "Success", "data": {"balance": new_balance, "currency": "TND", "reference_id": tx_id, "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}
    except Exception as e:
        return {"status_code": 500, "status_description": str(e)}

def get_eurovirtuals_headers(payload=None):
    if payload is None:
        payload = {
            "currency": "TND",
            
        }
    timestamp = str(int(time.time()))
    signature = hash_create(payload, EURO_APP_KEY)
    
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "x-api-key": EURO_API_KEY, 
        
        "x-signature": signature,
        "x-timestamp": timestamp
    }
   
@app.api_route("/api/get-eurovirtuals-games", methods=["GET"])
async def get_virtual_games():
    try:
        payload = {}
        timestamp = str(int(time.time()))
        signature = hash_create(payload, EURO_APP_KEY)
        
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "x-api-key": EURO_API_KEY,
            "x-signature-key": signature,
            "x-timestamp": timestamp
        }
        
        base_url_clean = str(EURO_BASE_URL).rstrip('/')
        games_endpoint = f"{base_url_clean}/v1/games"
        print(f"🔗 CONNECTING TO URL: {games_endpoint}")
        print(f"🔑 USING API KEY: {EURO_API_KEY[:10]}...")
        
        try:
            response = requests.get(games_endpoint, headers=headers, timeout=20)
            data = response.json()
            print(f"🎮 RAW GAMES FROM PROVIDER: {data}")
        except Exception:
            return {"status": "error", "error": "Invalid JSON response", "details": response.text}
        
        if response.status_code == 200 and data.get("status_code") == 200:
            games_list = data.get("data", {}).get("data", [])
            if games_list:
                print(f"🎮 FIRST GAME UUID FROM PROD: {games_list[0].get('uuid') or games_list[0].get('game_uuid')}")
            
            for game in games_list:
                image_url = game.get("logo") or game.get("thumbnail") or ""
                if image_url:
                    game["image"] = image_url
                    game["img"] = image_url
                
                game["game_code"] = game.get("uuid") or game.get("game_uuid") or game.get("id")
                    
            return {"status": "success", "games": games_list}
        else:
            return {"status": "error", "error": data.get("status_description", "Unknown Error"), "full_data": data}

    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.post("/api/provider/launch-eurovirtuals")
async def launch_eurovirtuals(request: Request):
    try:
        data = await request.json()
        print(f"🚨 1. EXACT DATA RECEIVED: {data}")
        
        game_uuid = str(data.get("game_uuid") or data.get("game_code") or data.get("id") or "")
        print(f"🎯 2. EXTRACTED game_uuid: '{game_uuid}'")
        
        if not game_uuid or game_uuid == "undefined":
            return {"error": "Game UUID manquant"}
            
        user_code = str(data.get("user_code", "test_user"))
        print(f"👤 3. USER CODE: {user_code}")
        
        async with db_lock:
            db = load_db()
            target_user = next((u for u in db if str(u.get("username", "")).lower().strip() == user_code.lower().strip()), None)
            
            if not target_user:
                print("❌ 4. ERROR: Player not found in DB")
                return {"error": "Joueur introuvable"}
            if target_user.get("is_blocked") == 1:
                print("❌ 4. ERROR: Player is blocked")
                return {"error": "Joueur bloqué"}
                
            current_balance = float(target_user.get("balance", 0.0))
            print(f"💰 5. BALANCE FOUND: {current_balance}")

        payload = {
            "player_id": user_code,
            "player_name": user_code,
            "player_token": f"tok_{user_code}",
            "currency": "TND",
            "demo": 0,
            "game_uuid": game_uuid,
            "balance": current_balance,
            "country": "TN",
            "language": "fr",
            "device": "desktop"
        }
        print(f"📦 6. PAYLOAD PREPARED: {payload}")

        signature = hash_create(payload, EURO_APP_KEY)
        print(f"✍️ 7. SIGNATURE GENERATED: {signature}")

        headers = {
            "Accept": "application/json",
            "x-api-key": EURO_API_KEY,
            "x-signature-key": signature,
            "x-signature": signature,
            "x-timestamp": str(int(time.time())),
            "Content-Type": "application/json"
        }

        base_url_clean = str(EURO_BASE_URL).rstrip('/')
        launch_endpoint = f"{base_url_clean}/v1/launch"
        print(f"🚀 8. SENDING TO: {launch_endpoint}")
        
        async with httpx.AsyncClient() as client:
            response = await client.post(launch_endpoint, json=payload, headers=headers, timeout=20)
            print(f"🌐 9. RAW STATUS: {response.status_code}")
            print(f"🌐 10. RAW TEXT: {response.text}")
            
            try:
                response_data = response.json()
            except Exception:
                return {"error": "JSON non valide du fournisseur", "details": response.text}

            if response_data.get("status_code") == 200:
                game_url = response_data.get("data", {}).get("url")
                if game_url and game_url.startswith("/"):
                    game_url = f"{base_url_clean}{game_url}"
                return {"launch_url": game_url}
            else:
                return {"error": response_data.get("status_description", "Rejected"), "details": response_data}

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"🔥 CRITICAL CRASH: {error_details}")
        return {"error": str(e)}
    
@app.post("/api/provider/launch-sportsbook")
async def launch_sportsbook(request: Request):
    try:
        data = await request.json()
        provider_code = str(data.get("provider_code", "")).lower()
        user_code = str(data.get("user_code", "test_user"))
        provider_user_code = f"{SITE_PREFIX}{user_code}" 
        
        if provider_code == "smpl":
            payload = {
                "sportsbook_uuid": "YOUR_SPORTSBOOK_UUID_HERE", 
                "currency": "TND",
                "session_id": f"sess_{uuid.uuid4().hex[:10]}",
                "player_id": provider_user_code, 
                "player_name": provider_user_code,
                "return_url": "https://alphabet216.com/"
            }
            
            headers = get_smpl_headers_and_sign(payload)
            headers['Content-Type'] = 'application/json'
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{SMPL_BASE_URL}/sportsbooks/init", 
                    json=payload,
                    headers=headers,
                    timeout=20
                )
                res_data = response.json()
                if "url" in res_data:
                    return {"launch_url": res_data["url"]}
                else:
                    return {"error": "Erreur d'initialisation SMPL", "details": res_data}

        else:
            payload = {
                "method": "game_launch",
                "agent_code": AGENT_CODE,
                "agent_token": AGENT_TOKEN,
                "provider_code": str(data.get("provider_code", "SPORTSBOOK")), 
                "game_code": str(data.get("game_code", "SPORTSBOOK")),
                "user_code": provider_user_code, 
                "lang": "fr",
                "lobby_url": "https://alphabet216.com/"
            }
            
            headers = {"Content-Type": "application/json"}
            endpoint = PROVIDER_ENDPOINT.rstrip('/')
            
            async with httpx.AsyncClient() as client:
                response = await client.post(endpoint, json=payload, headers=headers, timeout=20)
                try:
                    response_data = response.json()
                except Exception:
                    return {"error": "Le fournisseur n'a pas renvoyé de réponse JSON valide", "details": response.text}
                    
                game_url = response_data.get("url") or response_data.get("launch_url") or (response_data.get("data", {}).get("url"))
                
                if game_url:
                    return {"launch_url": game_url}
                else:
                    return {"error": "Demande refusée par le fournisseur", "details": response_data}
                
    except Exception as e:
        print(f"❌ [CRITICAL ERROR IN LAUNCH SPORTSBOOK]: {str(e)}")
        return {"error": str(e)}

@app.get("/api/get-sportsbook-uuid")
async def fetch_sportsbook_uuid():
        headers = get_smpl_headers_and_sign()
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{SMPL_BASE_URL}/sportsbooks", 
                    headers=headers,
                    timeout=15
                )
                data = response.json()
                return {"status": "success", "data_from_smpl": data}
            except Exception as e:
                return {"status": "error", "details": str(e)}
                
class HandleHugeWinRequest(BaseModel):
    tx_id: int
    decision: str 

@app.get("/api/admin/pending-huge-wins")
async def get_pending_huge_wins(current_user: str = Depends(get_admin_user)):
    db_session = SessionLocal()
    try:
        txs = db_session.query(Transaction).filter(Transaction.admin_username == "PENDING_HUGE_WIN").all()
        return [{"id": t.id, "target_username": t.target_username, "amount": float(t.amount), "action": t.action, "date": t.date} for t in txs]
    finally:
        db_session.close()

@app.post("/api/admin/handle-huge-win")
async def handle_huge_win(req: HandleHugeWinRequest, current_user: str = Depends(get_admin_user)):
    db = load_db()
    admin = next((u for u in db if u["username"] == current_user), None)
    if not admin or admin.get("role") not in ["owner", "super_admin"]:
        raise HTTPException(status_code=403, detail="Privilèges de propriétaire requis")

    db_session = SessionLocal()
    try:
        tx = db_session.query(Transaction).filter(Transaction.id == req.tx_id).first()
        if not tx or tx.admin_username != "PENDING_HUGE_WIN":
            return JSONResponse(status_code=404, content={"detail": "Demande introuvable ou déjà traitée"})

        if req.decision == "approve":
            async with db_lock:
                target_user = next((u for u in db if str(u["username"]).lower() == str(tx.target_username).lower()), None)
                if target_user:
                    target_user["balance"] = round(float(target_user.get("balance", 0)) + tx.amount, 2)
                    save_db(db)
            tx.admin_username = f"APPROVED_BY_{current_user.upper()}"
        else:
            tx.admin_username = f"REJECTED_BY_{current_user.upper()}"

        db_session.commit()
        return {"status": "success", "message": "Gain massif traité avec succès"}
    finally:
        db_session.close()           

from pydantic import BaseModel
from datetime import datetime, timedelta
import uuid

class FreeSpinRequest(BaseModel):
    target_username: str
    provider_code: str
    game_code: str
    spin_count: int
    amount: float
    bet_level: int = 1

@app.post("/api/admin/grant-freespins")
async def grant_free_spins(req: FreeSpinRequest, current_user: str = Depends(get_admin_user)):
    db = load_db()
    admin = next((u for u in db if u["username"] == current_user), None)
    if not admin or admin.get("role") not in ["owner", "super_admin"]:
        raise HTTPException(status_code=403, detail="Privilèges de propriétaire requis")

    expiration = (datetime.utcnow() + timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    tour_id = f"fs_{uuid.uuid4().hex[:10]}"

    payload = {
        "method": "tour_create",
        "agent_code": AGENT_CODE, 
        "agent_token": AGENT_TOKEN,
        "user_code": req.target_username,
        "provider_code": req.provider_code,
        "game_code": req.game_code,
        "bet_level": req.bet_level,
        "spin_count": req.spin_count,
        "amount": req.amount,
        "expiration_time": expiration,
        "tour_id": tour_id
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(GOLD_API_URL, json=payload, timeout=15.0)
            res_data = response.json()
            
            if res_data.get("status") == 1:
                real_win = res_data.get("real_win", 0)
                return {"status": "success", "message": f"{req.spin_count} tours envoyés !", "real_win": real_win}
            else:
                error_msg = res_data.get("detail") or res_data.get("msg") or "Le fournisseur a rejeté la demande"
                return JSONResponse(status_code=400, content={"detail": f"Erreur du fournisseur : {error_msg}"})
        except Exception as e:
            return JSONResponse(status_code=500, content={"detail": f"Erreur de connexion : {str(e)}"}) 
        
class CancelTourRequest(BaseModel):
            tour_id: str

@app.post("/api/admin/cancel-freespins")
async def cancel_free_spins(req: CancelTourRequest, current_user: str = Depends(get_admin_user)):
    db = load_db()
    admin = next((u for u in db if u["username"] == current_user), None)
    if not admin or admin.get("role") not in ["owner", "super_admin"]:
        raise HTTPException(status_code=403, detail="Privilèges de propriétaire requis")

    payload = {
        "method": "tour_cancel",
        "agent_code": AGENT_CODE,
        "agent_token": AGENT_TOKEN,
        "tour_id": req.tour_id
    }

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(GOLD_API_URL, json=payload, timeout=15.0)
            res_data = response.json()
            
            if res_data.get("status") == 1:
                refund = res_data.get("canceled_money", 0)
                rest = res_data.get("rest_count", 0)
                return {"status": "success", "message": f"Annulé ! Remboursement : {refund} TND (reste {rest} tours non joués)."}
            else:
                error_msg = res_data.get("detail") or res_data.get("msg") or "Le fournisseur a rejeté la demande"
                return JSONResponse(status_code=400, content={"detail": f"Erreur du fournisseur : {error_msg}"})
        except Exception as e:
            return JSONResponse(status_code=500, content={"detail": f"Erreur de connexion : {str(e)}"})
			
import json
import os
from pydantic import BaseModel
from typing import Optional

PROMO_FILE = "promo_config.json"

def load_promo():
    if os.path.exists(PROMO_FILE):
        with open(PROMO_FILE, "r") as f: return json.load(f)
    return {"is_active": False, "day_of_week": "Friday", "min_amount": 50, "spins": 10, "game": "vs20doghouse", "provider": "PRAGMATIC", "max_win": 10000}

class PromoModel(BaseModel):
    is_active: bool
    day_of_week: str
    min_amount: float
    spins: int
    game: str
    provider: str
    max_win: float

@app.get("/api/admin/promo")
async def get_promo(current_user: str = Depends(get_admin_user)):
    return load_promo()

@app.post("/api/admin/promo")
async def set_promo(data: PromoModel, current_user: str = Depends(get_admin_user)):
    db = load_db()
    admin = next((u for u in db if u["username"] == current_user), None)
    if not admin or admin.get("role") not in ["owner", "super_admin"]:
        raise HTTPException(status_code=403, detail="Refusé")
    
    with open(PROMO_FILE, "w") as f: json.dump(data.dict(), f)
    return {"status": "success", "message": "Paramètres de la promotion enregistrés avec succès"}    

@app.get("/api/admin/fraud-detection")
async def fraud_detection(current_user: str = Depends(get_admin_user)):
    db = load_db()
    ip_map = {}
    phone_map = {}
    
    for u in db:
        ip = u.get("last_ip") or u.get("ip")
        phone = u.get("phone")
        uname = u.get("username")
        
        if ip:
            ip_map.setdefault(ip, []).append(uname)
        if phone and phone != "00000000" and phone != "":
            phone_map.setdefault(phone, []).append(uname)
            
    suspicious_ips = {ip: users for ip, users in ip_map.items() if len(users) > 1}
    suspicious_phones = {phone: users for phone, users in phone_map.items() if len(users) > 1}
    
    return {
        "status": "success",
        "shared_ips": suspicious_ips,
        "shared_phones": suspicious_phones
    }
    
@app.get("/api/admin/analytics/ggr")
async def get_ggr_analytics(
    year: Optional[int] = Query(None), 
    month: Optional[int] = Query(None), 
    current_user: str = Depends(get_admin_user)
):
    db_session = SessionLocal()
    try:
        txs = db_session.query(Transaction).filter(
            Transaction.action.in_(["bet", "win", "rollback"])
        ).all()
        
        filtered_txs = []
        for t in txs:
            if not t.date:
                continue
            try:
                date_part = str(t.date).split(" ")[0]
                parts = date_part.split("-")
                if len(parts) >= 2:
                    t_year = int(parts[0])
                    t_month = int(parts[1])
                    
                    if year and t_year != year:
                        continue
                    if month and t_month != month:
                        continue
            except Exception:
                pass
            filtered_txs.append(t)
        
        total_bets = sum(t.amount for t in filtered_txs if t.action == "bet")
        total_wins = sum(t.amount for t in filtered_txs if t.action == "win")
        
        ggr = total_bets - total_wins
        actual_rtp = (total_wins / total_bets * 100) if total_bets > 0 else 0.0
        
        return {
            "status": "success",
            "total_bets": round(total_bets, 2),
            "total_wins": round(total_wins, 2),
            "ggr": round(ggr, 2),
            "actual_rtp": round(actual_rtp, 2)
        }
    except Exception as e:
        print(f"Analytics Error: {e}")
        return {"status": "error", "total_bets": 0, "total_wins": 0, "ggr": 0, "actual_rtp": 0}
    finally:
        db_session.close()
        
@app.get("/api/admin/casino-history")
async def get_casino_history(current_user: str = Depends(get_admin_user)):
    db = load_db()
    current_admin = next((u for u in db if u["username"] == current_user), None)
    current_role = current_admin.get("role", "player")

    if current_role in ["owner", "system"]:
        allowed_users = {u["username"] for u in db}
    else:
        allowed_users = {current_user}
        to_process = [current_user]
        while to_process:
            parent = to_process.pop(0)
            children = [u["username"] for u in db if u.get("created_by") == parent]
            for child in children:
                if child not in allowed_users:
                    allowed_users.add(child)
                    to_process.append(child)

    db_session = SessionLocal()
    try:
        txs = db_session.query(Transaction).filter(
            Transaction.action.in_(["bet", "win", "rollback"])
        ).all()
        
        result = []
        for t in txs:
            username = t.username or t.target_username
            if username in allowed_users:
                result.append({
                    "id": t.id,
                    "username": username,
                    "action": t.action,
                    "amount": t.amount,
                    "date": str(t.date)
                })
                
        result.reverse() 
        return result[:500] 
    finally:
        db_session.close()
        
class AuditLog(Base):
    __tablename__ = "audit_logs"
    id = Column(Integer, primary_key=True, index=True)
    admin_username = Column(String, index=True)
    action_type = Column(String)
    details = Column(String)
    date = Column(String, default=lambda: str(datetime.now()))

def log_admin_action(admin_username: str, action_type: str, details: str):
    db_session = SessionLocal()
    try:
        log_entry = AuditLog(
            admin_username=admin_username,
            action_type=action_type,
            details=details,
            date=str(datetime.now())
        )
        db_session.add(log_entry)
        db_session.commit()
    except Exception as e:
        print(f"Audit Log Error: {e}")
    finally:
        db_session.close()
        
@app.get("/api/admin/audit-logs")
async def get_audit_logs(current_user: str = Depends(get_admin_user)):
    db = load_db()
    current_admin = next((u for u in db if u["username"] == current_user), None)
    current_role = current_admin.get("role", "player")

    if current_role in ["owner", "system"]:
        allowed_users = {u["username"] for u in db}
    else:
        allowed_users = {current_user}
        to_process = [current_user]
        while to_process:
            parent = to_process.pop(0)
            children = [u["username"] for u in db if u.get("created_by") == parent]
            for child in children:
                if child not in allowed_users:
                    allowed_users.add(child)
                    to_process.append(child)

    db_session = SessionLocal()
    try:
        logs = db_session.query(AuditLog).order_by(AuditLog.id.desc()).all()
        result = []
        for l in logs:
            if l.admin_username in allowed_users:
                result.append({
                    "id": l.id,
                    "admin_username": l.admin_username,
                    "action_type": l.action_type,
                    "details": l.details,
                    "date": l.date
                })
        return result[:300] 
    finally:
        db_session.close()
        
import random
from datetime import datetime, timedelta
import asyncio
import uuid

JACKPOTS_BASE = {
    "mini":  {"start": 20.0, "days": 1},
    "minor": {"start": 40.0, "days": 2},
    "major": {"start": 80.0, "days": 7},
    "grand": {"start": 500.0, "days": 30},
}

def generate_drop_time(days):
    now = datetime.now()
    random_seconds = random.randint(1, int(timedelta(days=days).total_seconds()))
    return now + timedelta(seconds=random_seconds)

jackpots_state = {
    "mini":  {"current_amount": 20.0, "drop_time": generate_drop_time(1)},
    "minor": {"current_amount": 40.0, "drop_time": generate_drop_time(2)},
    "major": {"current_amount": 80.0, "drop_time": generate_drop_time(7)},
    "grand": {"current_amount": 500.0, "drop_time": generate_drop_time(30)},
}

async def time_based_jackpot_engine():
    await asyncio.sleep(10) 
    while True:
        try:
            now = datetime.now()
            for level in jackpots_state:
                jackpots_state[level]["current_amount"] += 0.03
                
                if now >= jackpots_state[level]["drop_time"]:
                    await trigger_jackpot_drop(level, jackpots_state[level]["current_amount"])
                    
                    jackpots_state[level]["current_amount"] = JACKPOTS_BASE[level]["start"]
                    jackpots_state[level]["drop_time"] = generate_drop_time(JACKPOTS_BASE[level]["days"])
                    
        except Exception as e:
            print(f"❌ Jackpot Engine Error: {e}")
        
        await asyncio.sleep(60) 

async def trigger_jackpot_drop(level, total_amount):
    async with db_lock:
        db = load_db()
        
        eligible_users = [
            u for u in db 
            if str(u.get("role")) == "player" 
            and str(u.get("is_blocked")) != "1" 
            and (float(u.get("balance", 0.0)) > 0 or u.get("last_spin_date", "") != "")
        ]
        
        if not eligible_users:
            return 
        
        winners = random.sample(eligible_users, min(5, len(eligible_users)))
        win_per_user = round(total_amount / len(winners), 2)
        
        db_session = SessionLocal()
        try:
            for winner in winners:
                winner["balance"] = round(float(winner.get("balance", 0.0)) + win_per_user, 2)
                
                new_tx = Transaction(
                    admin_username="SYSTEM_JACKPOT",
                    target_username=winner["username"],
                    action=f"jackpot_win ({level})",
                    amount=win_per_user,
                    date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    tx_id=f"jp_{uuid.uuid4().hex[:8]}"
                )
                db_session.add(new_tx)
                
                if "notifications" not in db.full_data:
                    db.full_data["notifications"] = []
                new_notif = {
                    "id": str(uuid.uuid4())[:8],
                    "target": winner["username"],
                    "title": f"🎉 Jackpot {level.upper()} !",
                    "message": f"Félicitations! Vous avez gagné une part du jackpot. {win_per_user} TND ont été ajoutés à votre compte.",
                    "icon": "fa-coins",
                    "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "read_by": []
                }
                db.full_data["notifications"].append(new_notif)
                
            db_session.commit()
            save_db(db)
            print(f"🎉 JACKPOT {level.upper()} DROP! {total_amount} TND divided among {len(winners)} players.")
            
            try:
                masked_winners = [f"{str(w['username'])[:4]}***" for w in winners]
                drop_data = {
                    "type": "jackpot_drop",
                    "level": level.upper(),
                    "total_amount": total_amount,
                    "win_per_user": win_per_user,
                    "winners": masked_winners
                }
                asyncio.create_task(jackpot_manager.broadcast(json.dumps(drop_data)))
            except Exception as e:
                print(f"Error broadcasting jackpot drop: {e}")
                
        except Exception as e:
            db_session.rollback()
            print(f"❌ Error distributing jackpot: {e}")
        finally:
            db_session.close()
			
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except:
                pass

jackpot_manager = ConnectionManager()

@app.websocket("/ws/jackpot")
async def websocket_jackpot(websocket: WebSocket):
    await jackpot_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        jackpot_manager.disconnect(websocket)

async def broadcast_jackpots():
    while True:
        live_data = {
            "mini": round(jackpots_state["mini"]["current_amount"], 2),
            "minor": round(jackpots_state["minor"]["current_amount"], 2),
            "major": round(jackpots_state["major"]["current_amount"], 2),
            "grand": round(jackpots_state["grand"]["current_amount"], 2)
        }
        await jackpot_manager.broadcast(json.dumps(live_data))
        await asyncio.sleep(2)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(broadcast_jackpots())
    asyncio.create_task(time_based_jackpot_engine()) 


class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except:
                pass

jackpot_manager = ConnectionManager()

@app.websocket("/ws/jackpot")
async def websocket_jackpot(websocket: WebSocket):
    await jackpot_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text() 
    except WebSocketDisconnect:
        jackpot_manager.disconnect(websocket)

async def broadcast_jackpots():
    while True:
        live_data = {
            "mini": jackpots_state["mini"]["current_amount"],
            "minor": jackpots_state["minor"]["current_amount"],
            "major": jackpots_state["major"]["current_amount"],
            "grand": jackpots_state["grand"]["current_amount"]
        }
        await jackpot_manager.broadcast(json.dumps(live_data))
        await asyncio.sleep(2) 

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(broadcast_jackpots())
    
class NotificationModel(BaseModel):
    target_user: str  
    title: str
    message: str
    icon: str = "fa-bell" 

@app.post("/api/admin/send-notification")
async def send_notification(req: NotificationModel, current_user: str = Depends(get_admin_user)):
    db = load_db()
    if "notifications" not in db.full_data:
        db.full_data["notifications"] = []
    
    new_notif = {
        "id": str(uuid.uuid4())[:8],
        "target": req.target_user.lower().strip(),
        "title": req.title,
        "message": req.message,
        "icon": req.icon,
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "read_by": []
    }
    db.full_data["notifications"].append(new_notif)
    save_db(db)
    return {"status": "success", "message": "Notification envoyée avec succès"}

@app.get("/api/user/notifications")
async def get_user_notifications(current_user: str = Depends(get_current_user)):
    db = load_db()
    notifs = db.full_data.get("notifications", [])
    user_notifs = []
    unread_count = 0
    
    for n in notifs:
        if current_user.lower() in n.get("deleted_by", []):
            continue
            
        if n.get("target") == "all" or n.get("target") == current_user.lower():
            is_read = current_user.lower() in n.get("read_by", [])
            if not is_read:
                unread_count += 1
            user_notifs.append({**n, "is_read": is_read})
            
    return {"unread": unread_count, "notifications": user_notifs[::-1][:15]}

class MarkReadModel(BaseModel):
    notif_id: str

@app.post("/api/user/read-notification")
async def mark_notif_read(req: MarkReadModel, current_user: str = Depends(get_current_user)):
    db = load_db()
    notifs = db.full_data.get("notifications", [])
    for n in notifs:
        if n["id"] == req.notif_id:
            if current_user.lower() not in n.get("read_by", []):
                n.setdefault("read_by", []).append(current_user.lower())
            break
    save_db(db)
    return {"status": "success"}

class DeleteNotifModel(BaseModel):
    notif_id: str

@app.post("/api/user/delete-notification")
async def delete_notification(req: DeleteNotifModel, current_user: str = Depends(get_current_user)):
    db = load_db()
    notifs = db.full_data.get("notifications", [])
    
    for n in notifs:
        if req.notif_id == "all":
            if n.get("target") in ["all", current_user.lower()]:
                if current_user.lower() not in n.get("deleted_by", []):
                    n.setdefault("deleted_by", []).append(current_user.lower())
        else:
            if n["id"] == req.notif_id:
                if current_user.lower() not in n.get("deleted_by", []):
                    n.setdefault("deleted_by", []).append(current_user.lower())
                break
                
    save_db(db)
    return {"status": "success"}

@app.get("/setup-owner-fethi")
async def setup_owner():
    try:
        # تفريغ قاعدة البيانات بالكامل من كل الحسابات القديمة
        db = [] 
        
        # إنشاء حساب الأونر الجديد والنظيف
        owner_account = {
            "id": 1,
            "username": "fethi",
            "password": hash_password("fethi2026"), # سيتم تشفيرها تلقائياً
            "role": "owner",
            "balance": 1000000.0, # رصيد المنصة الأساسي
            "phone": "00000000",
            "created_by": "system",
            "is_blocked": 0,
            "rtp": 50
        }
        
        db.append(owner_account)
        save_db(db) # حفظ قاعدة البيانات بعد التنظيف
        
        return {"message": "تم تنظيف النظام بالكامل، وتم إنشاء حساب الأونر (fethi) بنجاح!"}
    except Exception as e:
        return {"error": str(e)}