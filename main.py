from fastapi import FastAPI, HTTPException, Depends, Request, UploadFile, File, Form, Header, Body, Query, WebSocket, WebSocketDisconnect
from fastapi.security import OAuth2PasswordBearer
import requests
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
db_lock = asyncio.Lock()
import shutil
from fastapi.staticfiles import StaticFiles
import httpx
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse, RedirectResponse, StreamingResponse
from dotenv import load_dotenv
import qrcode
import io
import html
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import uuid

PROCESSED_TRANSACTIONS = set()

class SportsLaunchRequest(BaseModel):
    provider_code: str
    game_code: str
    user_code: str

# ==========================================
# 🎮 إعدادات الكازينو (NexusGGR)
# ==========================================
AGENT_CODE = os.getenv("AGENT_CODE", "XD24")
AGENT_TOKEN = os.getenv("AGENT_TOKEN", "")
NEXUS_SECRET_KEY = os.getenv("NEXUS_SECRET_KEY", "")
PROVIDER_ENDPOINT = os.getenv("PROVIDER_ENDPOINT", "https://api.nexusggr.eu")

# ==========================================
# ⚽ إعدادات الألعاب الافتراضية (EuroVirtuals)
# ==========================================
EURO_APP_KEY = os.getenv("EURO_APP_KEY", "")
EURO_API_KEY = os.getenv("EURO_API_KEY", "")
EURO_BASE_URL = os.getenv("EURO_BASE_URL", "https://api.betkraft.co.uk/")

load_dotenv()
ADMIN_USER = os.getenv("ADMIN_USERNAME")
ADMIN_PASS = os.getenv("ADMIN_PASSWORD")
SECRET_KEY = os.getenv("SECRET_KEY", "alpha-secure-key-2026")

# 1. إعداد الاتصال بـ Firebase بشكل آمن لمنع انهيار السيرفر
try:
    if not firebase_admin._apps:
        if os.path.exists("firebase-key.json"):
            cred = credentials.Certificate("firebase-key.json") 
            firebase_admin.initialize_app(cred, {
                'databaseURL': 'https://xdanous-5a6c4-default-rtdb.firebaseio.com/'
            })
        else:
            print("⚠️ تحذير: ملف firebase-key.json غير موجود.")
except Exception as e:
    print(f"❌ خطأ في تهيئة Firebase: {e}")

def load_db():
    data = None
    try:
        ref = db.reference('/') 
        data = ref.get()
    except Exception as e:
        print(f"⚠️ تحذير: تعذر الاتصال بـ Firebase: {e}")
    
    if data is None:
        data = {"users": [], "shop_withdrawals": [], "tickets": []}
    
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
    try:
        ref = db.reference('/')
        if hasattr(data, 'full_data'):
            data.full_data['users'] = list(data)
            ref.set(data.full_data)
        elif isinstance(data, list):
            ref.child('users').set(list(data))
        else:
            ref.set(data)
    except Exception as e:
        print(f"⚠️ خطأ في الحفظ السحابي: {e}")

DB_FILE = "tickets_database.json"
TICKETS_FILE = "tickets_database.json" 

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./local_test.db")
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class User(Base):
    __tablename__ = "tounsibet_users"
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
        raise HTTPException(status_code=403, detail="Access Denied: Admin privileges required")
    
    return current_user

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

limiter = Limiter(key_func=get_remote_address)

app = FastAPI()

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
from starlette.middleware.sessions import SessionMiddleware
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://www.xdanous.net",
        "https://xdanous.net",
        "https://xdanous-player-frontend.onrender.com",
        "http://localhost:5500",
        "http://127.0.0.1:5500"
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
        print(f"Telegram Alert Error: {e}")

def verify_nexus_ip(request: Request):
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        client_ip = forwarded_for.split(",")[0].strip()
    else:
        client_ip = request.client.host
    return client_ip

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
    if not ticket:
        raise HTTPException(status_code=404, detail="التذكرة غير موجودة")
    
    old_status = ticket.get("status")
    player_username = ticket.get("username")
    win_amount = float(ticket.get("gain", 0))

    target_user = next((u for u in db if u["username"] == player_username), None)
    if not target_user:
        raise HTTPException(status_code=404, detail="اللاعب غير موجود")

    if old_status == "gagne" and req.new_status != "gagne":
        target_user["balance"] = float(target_user.get("balance", 0)) - win_amount
    elif old_status != "gagne" and req.new_status == "gagne":
        target_user["balance"] = float(target_user.get("balance", 0)) + win_amount

    ticket["status"] = req.new_status
    save_tickets_db(tickets_db)
    save_db(db)
    log_admin_action(current_user, "RESET_TICKET", f"Ticket ID {req.ticket_id} changed to {req.new_status}")
    
    return {"status": "success", "message": f"تم تعديل التذكرة بنجاح إلى {req.new_status}"}    

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
        
        alert_msg = f"🚨 <b>عملية إيداع جديدة!</b>\n👤 اللاعب: <code>{new_ticket['username']}</code>\n💰 المبلغ: <b>{new_ticket['amount']}</b>\n💳 الطريقة: {new_ticket['method']}"
        asyncio.create_task(send_telegram_alert(alert_msg))
 
        with open(TICKETS_FILE, "w", encoding="utf-8") as f:
            json.dump(db, f, indent=4, ensure_ascii=False)
            
        return {"status": "success", "message": "تم إرسال طلب الإيداع بنجاح"}
    except Exception as e:
        print(f"Error in create_deposit: {e}")
        return {"status": "error", "message": "حدث خطأ أثناء معالجة الطلب"}

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
        print(f"Error GET withdrawals: {e}")
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
            return JSONResponse(status_code=404, content={"detail": "Demande introuvable"})
            
        if tx.admin_username != "PENDING":
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
                "code": code_val if code_val != "FILE" else "مرفق صورة",
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
            raise HTTPException(status_code=404, detail="التذكرة غير موجودة")
            
        if ticket.get("status") != "pending":
            raise HTTPException(status_code=400, detail="هذه التذكرة تمت معالجتها مسبقاً")

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

        return {"status": "success", "message": f"تمت الموافقة وإضافة {real_amount} بنجاح"}
    except Exception as e:
        print(f"Error approving deposit: {e}")
        raise HTTPException(status_code=500, detail="حدث خطأ داخلي أثناء الموافقة")

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
            print(f"❌ [Auto-Settler] حدث خطأ: {e}")
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
                await asyncio.sleep(3600)
            else:
                await asyncio.sleep(300)
        except Exception as e:
            print(f"❌ [Cashback] حدث خطأ: {e}")
            await asyncio.sleep(300) 

class LoginRequest(BaseModel): username: str; password: str
class RegisterRequest(BaseModel): username: str; password: str; role: str; created_by: str; phone: str = ""
class ConfigureAccountRequest(BaseModel): admin_username: str; target_username: str; rtp: int; is_blocked: int
class UpdateBalanceRequest(BaseModel): admin_username: str; target_username: str; action: str; amount: float
class ChangePlayerPasswordRequest(BaseModel): admin_username: str; target_username: str; new_password: str
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
        safe_users.append(safe_user)
        
    return safe_users

@app.post("/api/admin/update-balance")
async def update_balance(req: UpdateBalanceRequest, current_user: str = Depends(get_admin_user)):
    target = req.target_username.lower().strip()
    amount = float(req.amount)

    if amount <= 0:
        raise HTTPException(status_code=400, detail="Montant invalide")

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
            raise HTTPException(status_code=403, detail="Accès refusé.")

        if req.action == "charge":
            if not is_global_admin:
                if float(admin_user.get("balance", 0)) < amount: 
                    raise HTTPException(status_code=400, detail="Solde insuffisant")
                admin_user["balance"] = round(float(admin_user.get("balance", 0)) - amount, 2)
            
            target_user["balance"] = round(float(target_user.get("balance", 0)) + amount, 2)
            if current_user.lower() != "system":
                target_user["daily_deposits"] = float(target_user.get("daily_deposits", 0)) + amount

        elif req.action == "withdraw":
            if float(target_user.get("balance", 0)) < amount: 
                raise HTTPException(status_code=400, detail="Solde insuffisant")
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
            raise HTTPException(status_code=500, detail="Erreur base de données.")
        finally:
            db_session.close()

        save_db(db)
        
    log_admin_action(current_user, "BALANCE_UPDATE", f"Target: {target}, Action: {req.action}, Amount: {amount}")
    return {"status": "success", "message": "Opération réussie"}

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
        print(f"Error: {e}")
    finally:
        db_session.close()

    history.sort(key=lambda x: x["timestamp"], reverse=True)
    return {"status": "success", "data": history}

@app.post("/api/admin/request-transaction")
async def request_transaction(request: Request):
    db_session = SessionLocal()
    try:
        form = await request.form()
        target_username = form.get("target_username")
        action = form.get("action")
        amount = float(form.get("amount", 0))
        tx_id = form.get("tx_id", str(uuid.uuid4()))

        if amount <= 0:
            return JSONResponse(status_code=400, content={"detail": "Le montant doit être supérieur à zéro"})

        file_path = ""
        file = form.get("file")
        if file and isinstance(file, UploadFile) and file.filename:
            allowed_extensions = ['.png', '.jpg', '.jpeg', '.webp']
            allowed_mimes = ['image/png', 'image/jpeg', 'image/webp']
            file_ext = os.path.splitext(file.filename)[1].lower()
            if file_ext not in allowed_extensions or file.content_type not in allowed_mimes:
                return JSONResponse(status_code=400, content={"detail": "Format non autorisé"})
            os.makedirs("uploads", exist_ok=True)
            safe_filename = f"{uuid.uuid4().hex}{file_ext}"
            file_path = os.path.join("uploads", safe_filename)
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)

        new_tx_args = {
            "admin_username": "PENDING",
            "target_username": target_username,
            "action": action,
            "amount": amount,
            "tx_id": tx_id
        }
        if file_path:
            new_tx_args["image_path"] = file_path

        new_tx = Transaction(**new_tx_args)
        db_session.add(new_tx)
        db_session.commit()

        return {"status": "success", "message": "Demande envoyée"}
    except Exception as e:
        db_session.rollback()
        return JSONResponse(status_code=500, content={"detail": str(e)})
    finally:
        db_session.close()

@app.post("/api/admin/change-player-password")
async def change_player_password(req: ChangePlayerPasswordRequest):
    db = load_db()
    for u in db:
        if u["username"] == req.target_username.lower().strip():
            u["password"] = hash_password(req.new_password)
            save_db(db)
            return {"status": "success", "message": "Mot de passe modifié"}
    raise HTTPException(status_code=404, detail="Utilisateur non trouvé")

@app.post("/api/admin/configure-account")
async def configure_account(req: ConfigureAccountRequest):
    db = load_db()
    for u in db:
        if u["username"] == req.target_username.lower().strip():
            u["rtp"] = req.rtp
            u["is_blocked"] = req.is_blocked
            save_db(db)
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
        raise HTTPException(status_code=403, detail="Non autorisé")
        
    db = load_db()
    for u in db:
        if u["username"] == target_username:
            u["password"] = hash_password(req.new_password)
            save_db(db)
            return {"status": "success", "message": "Mot de passe modifié"}
    raise HTTPException(status_code=404, detail="Utilisateur non trouvé")

@app.get("/api/get-providers")
async def get_real_providers():
    payload = {"method": "provider_list", "agent_code": AGENT_CODE, "agent_token": AGENT_TOKEN}
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(PROVIDER_ENDPOINT, json=payload, timeout=15)
            return response.json()
        except Exception as e:
            return {"status": 0, "msg": "Error"}

GAMES_CACHE = {}
CACHE_TIME_LIMIT = 3600  

@app.post("/api/get-providers")
async def get_real_games(request: ProviderRequest):
    provider_code = request.provider_code
    current_time = time.time()
    
    if provider_code in GAMES_CACHE and (current_time - GAMES_CACHE[provider_code]['time']) < CACHE_TIME_LIMIT:
        return GAMES_CACHE[provider_code]['data']

    payload = {"method": "game_list", "agent_code": AGENT_CODE, "agent_token": AGENT_TOKEN, "provider_code": provider_code}
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(PROVIDER_ENDPOINT, json=payload, timeout=20)
            response_data = response.json()
            if response_data.get("status") == 1 or "games" in response_data:
                GAMES_CACHE[provider_code] = {'time': current_time, 'data': response_data}
            return response_data
        except Exception as e:
            if provider_code in GAMES_CACHE: return GAMES_CACHE[provider_code]['data']
            return {"status": 0, "msg": "Error"}

@app.post("/api/provider/launch-casino")
async def launch_casino(request: Request):
    try:
        data = await request.json()
        payload = {
            "method": "game_launch",
            "agent_code": AGENT_CODE,      
            "agent_token": AGENT_TOKEN,    
            "user_code": data.get("user_code", "test_user"),
            "provider_code": data.get("provider_code"),
            "game_code": data.get("game_code"),
            "lang": "fr",
            "lobby_url": "https://xdanous.com/#casino"
        }
        headers = {"Content-Type": "application/json"}
        endpoint = PROVIDER_ENDPOINT.rstrip('/')
        response = requests.post(endpoint, json=payload, headers=headers)
        response_data = response.json()
        
        if response.status_code == 200:
            game_url = response_data.get("url") or response_data.get("launch_url") or (response_data.get("data", {}).get("url"))
            if game_url:
                return {"launch_url": game_url}
        return {"error": "Erreur de lancement", "details": response_data}
    except Exception as e:
        return {"error": str(e)}

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

@app.post("/api/login")
@limiter.limit("5/minute")
async def login_user(request: Request, req: LoginRequest):
    try:
        uname = html.escape(req.username.lower().strip())
        db = load_db()
        user = next((u for u in db if u["username"] == uname), None)

        if not user or not verify_password(req.password, user.get("password", "")):
            return JSONResponse(status_code=401, content={"detail": "Identifiants incorrects"})
        user["last_ip"] = verify_nexus_ip(request)
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
        return JSONResponse(status_code=500, content={"detail": f"Erreur: {str(e)}"})

@app.post("/gold_api")
@app.post("/gold_api/gold_api")
async def seamless_wallet_handler(request: Request):
    try:
        data = await request.json()
        method, user_code = data.get("method"), data.get("user_code")
        
        db = load_db()
        target_user = next((u for u in db if str(u.get("username", "")).lower().strip() == str(user_code).lower().strip()), None)
        
        if not target_user:
            return JSONResponse(content={"status": 0, "msg": "USER_NOT_FOUND"})
        
        player_balance = float(target_user.get("balance", 0))

        if method == "user_balance":
            return JSONResponse(content={"status": 1, "user_balance": player_balance})

        elif method == "transaction":
            game_type = data.get("game_type")
            tx_data = data.get(game_type, {})
            bet_money = float(tx_data.get("bet_money", 0))
            win_money = float(tx_data.get("win_money", 0))
            txn_type = tx_data.get("txn_type")

            if txn_type in ["debit", "debit_credit"]:
                if player_balance < bet_money:
                    return JSONResponse(content={"status": 0, "msg": "INSUFFICIENT_USER_FUNDS"})
                player_balance -= bet_money

            if txn_type in ["credit", "debit_credit"]:
                player_balance += win_money

            target_user["balance"] = player_balance
            save_db(db)
            return JSONResponse(content={"status": 1, "user_balance": round(player_balance, 2)})

        else:
            return JSONResponse(content={"status": 0, "msg": "UNKNOWN_METHOD"})
    except Exception as e:
        return JSONResponse(content={"status": 0, "msg": "INTERNAL_ERROR"})

# Audit logs
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
        log_entry = AuditLog(admin_username=admin_username, action_type=action_type, details=details, date=str(datetime.now()))
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
    return {"status": "success"}

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

@app.post("/api/provider/launch-sportsbook")
async def launch_sportsbook(request: Request):
    try:
        data = await request.json()
        
        # استخراج وتصحيح كود المزود ليتوافق مع API
        provider_code = str(data.get("provider_code", "SPORTSBOOK")).upper()
        if provider_code == "NEXUS":
            provider_code = "SPORTSBOOK"
            
        user_code = str(data.get("user_code", "test_user"))
        
        payload = {
            "method": "game_launch",
            "agent_code": AGENT_CODE,
            "agent_token": AGENT_TOKEN,
            "provider_code": provider_code, 
            "game_code": "SPORTSBOOK",  # كود الرياضة الثابت المعتمد من المزود
            "user_code": user_code,
            "lang": "fr",
            "lobby_url": "https://www.xdanous.net/"
        }
        
        headers = {"Content-Type": "application/json"}
        endpoint = PROVIDER_ENDPOINT.rstrip('/')
        
        async with httpx.AsyncClient() as client:
            response = await client.post(endpoint, json=payload, headers=headers, timeout=20)
            
            try:
                response_data = response.json()
            except Exception:
                return {"error": "المزود لم يرسل رد JSON صالح", "details": response.text}
                
            # جلب الرابط من أي مسار محتمل في رد السيرفر
            game_url = response_data.get("url") or response_data.get("launch_url") or response_data.get("data", {}).get("url")
            
            if game_url:
                return {"launch_url": game_url}
            else:
                return {"error": "المزود رفض تشغيل قسم الرياضة", "details": response_data}
                
    except Exception as e:
        print(f"❌ Error launching sportsbook: {str(e)}")
        return {"error": str(e)}
