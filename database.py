import os
import uuid
from loguru import logger
from datetime import datetime
from typing import Dict, List, Optional
from supabase import create_client, Client

try:
    from db_tracker import track_db_operation
    _use_tracker = True
except ImportError:
    _use_tracker = False
    logger.warning("DB tracker not available")

class DatabaseManager:
    """Manages Supabase database operations"""
    
    def __init__(self):
        self.client: Optional[Client] = None
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_KEY")
    
    async def initialize(self):
        """Initialize Supabase client"""
        try:
            self.client = create_client(self.url, self.key)
            logger.info("Supabase client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Supabase: {e}")
            raise
    
    async def _get_or_create_user(self, name: str, phone_number: str) -> Dict:
        try:
            response = self.client.table("users").select("*").eq(
                "phone_number", phone_number
            ).execute()
            
            if response.data:
                return {"is_new": False, **response.data[0]}
            
            new_user = {
                "id": str(uuid.uuid4()),
                "name": name,
                "phone_number": phone_number,
                "created_at": datetime.now().isoformat(),
            }
            
            response = self.client.table("users").insert(new_user).execute()
            return {"is_new": True, **response.data[0]}
        
        except Exception as e:
            logger.error(f"Error in get_or_create_user: {e}")
            return {"is_new": True, "name": name, "phone_number": phone_number}
    
    async def _get_booked_slots(self, date: str) -> List[str]:
        try:
            response = self.client.table("appointments").select("time").eq(
                "date", date
            ).neq("status", "cancelled").execute()
            logger.info(f"Booked slots for {date}: {response.data}")
            slots = []
            for apt in response.data:
                slots.append(apt["time"])
            return slots
        
        except Exception as e:
            logger.error(f"Error fetching booked slots: {e}")
            return []
    
    async def _create_appointment(
        self,
        phone_number: str,
        date: str,
        time: str,
        service: str,
        notes: str = ""
    ) -> Dict:
        """Create a new appointment"""
        try:
            appointment = {
                "id": str(uuid.uuid4()),
                "phone_number": phone_number,
                "date": date,
                "time": time,
                "service": service,
                "notes": notes,
                "status": "active",
                "created_at": datetime.now().isoformat(),
            }
            
            response = self.client.table("appointments").insert(
                appointment
            ).execute()
            
            logger.info(f"Appointment created: {appointment['id']}")
            return response.data[0]
        
        except Exception as e:
            logger.error(f"Error creating appointment: {e}")
            raise
    
    async def _get_user_appointments(self, phone_number: str) -> List[Dict]:
        """Retrieve all appointments for a user"""
        try:
            response = self.client.table("appointments").select("*").eq(
                "phone_number", phone_number
            ).order("date", desc=False).order("time", desc=False).execute()
            
            return response.data
        
        except Exception as e:
            logger.error(f"Error retrieving appointments: {e}")
            return []
    
    async def _modify_appointment(
        self, 
        appointment_id: str, 
        new_date: str, 
        new_time: str
    ) -> bool:
        """Modify appointment date and time"""
        try:
            response = self.client.table("appointments").update(
                {
                    "date": new_date,
                    "time": new_time,
                    "updated_at": datetime.now().isoformat()
                }
            ).eq("id", appointment_id).execute()
            
            logger.info(f"Appointment modified: {appointment_id}")
            return len(response.data) > 0
        
        except Exception as e:
            logger.error(f"Error modifying appointment: {e}")
            return False
    
    async def _save_conversation_summary(
        self, 
        phone_number: str, 
        summary: Dict
    ) -> bool:
        """Save conversation summary to database"""
        try:
            summary_record = {
                "id": str(uuid.uuid4()),
                "phone_number": phone_number,
                "summary": summary,
                "created_at": datetime.now().isoformat(),
            }
            
            self.client.table("conversation_summaries").insert(
                summary_record
            ).execute()
            
            logger.info(f"Conversation summary saved for {phone_number}")
            return True
        
        except Exception as e:
            logger.error(f"Error saving conversation summary: {e}")
            return False


_db_manager: Optional[DatabaseManager] = None

def _get_db_manager() -> DatabaseManager:
    global _db_manager
    if _db_manager is None:
        _db_manager = DatabaseManager()
    return _db_manager

async def get_or_create_user_from_database(name: str, phone_number: str) -> Dict:
    async def _execute():
        try:
            db = _get_db_manager()
            if db.client is None:
                await db.initialize()
            
            result = await db._get_or_create_user(name, phone_number)
            logger.info(f"User found successfully: {result}")
            return result
        
        except Exception as e:
            logger.error(f"Error in get_or_create_user_from_database: {e}")
            return {
                "name": name,
                "phone_number": phone_number
            }
    
    if _use_tracker:
        return await track_db_operation(
            "get_or_create_user",
            _execute,
        )
    else:
        return await _execute()

async def book_appointment(phone_number: str, date: str, slot: str) -> Dict:
    async def _execute():
        try:
            db = _get_db_manager()

            if db.client is None:
                await db.initialize()

            booked_slots = await db._get_booked_slots(date)

            if slot in booked_slots:
                logger.warning(f"Slot {slot} on {date} is already booked")
                return {
                    "success": False,
                    "message": f"Slot {slot} on {date} is already booked"
                }

            appointment = await db._create_appointment(
                phone_number=phone_number,
                date=date,
                time=slot,
                service="General Appointment",
                notes=""
            )

            logger.info(
                f"Appointment booked: {appointment['id']} for {phone_number} "
                f"on {date} at {slot}"
            )

            return {
                "date": date,
                "time": slot,
                "success": True,
                "phone_number": phone_number,
                "appointment_id": appointment["id"],
                "status": appointment.get("status", "active")
            }

        except Exception as e:
            logger.error(f"Error booking appointment: {e}")
            return {
                "success": False,
                "message": f"Error booking appointment: {e}"
            }
    
    if _use_tracker:
        return await track_db_operation(
            "create_appointment",
            _execute,
        )
    else:
        return await _execute()

async def get_user_appointments_from_database(phone_number: str) -> List[Dict]:
    """Get all appointments for a user from database."""
    async def _execute():
        try:
            db = _get_db_manager()
            if db.client is None:
                await db.initialize()
            
            appointments = await db._get_user_appointments(phone_number)
            return appointments
        
        except Exception as e:
            logger.error(f"Error getting user appointments: {e}")
            return []
    
    if _use_tracker:
        return await track_db_operation(
            "get_user_appointments",
            _execute,
        )
    else:
        return await _execute()

async def save_conversation_summary_to_database(phone_number: str, summary: Dict) -> bool:
    """Save conversation summary to database."""
    try:
        db = _get_db_manager()
        if db.client is None:
            await db.initialize()
        
        success = await db._save_conversation_summary(phone_number, summary)
        return success
    
    except Exception as e:
        logger.error(f"Error saving conversation summary: {e}")
        return False

async def update_appointment_in_database(
        appointment_id: str, 
        date: str, 
        new_slot: str
    ) -> Dict:
    """Update appointment time to a new slot."""
    async def _execute():
        try:
            db = _get_db_manager()
            if db.client is None:
                await db.initialize()

            booked_slots = await db._get_booked_slots(date)
            current_appointment = db.client.table("appointments").select("time").eq(
                "id", appointment_id
            ).execute()
            
            current_slot = current_appointment.data[0].get("time")
            
            not_available_slots = [s for s in booked_slots if s != current_slot] if current_slot else booked_slots
            
            if new_slot in not_available_slots:
                logger.warning(f"Slot {new_slot} on {date} is already booked")
                return {
                    "success": False,
                    "message": f"Slot {new_slot} on {date} is already booked"
                }

            success = await db._modify_appointment(appointment_id, date, new_slot)
            
            if success:
                logger.info(
                    f"Appointment updated: {appointment_id} to {date} at {new_slot}"
                )
                return {
                    "success": True,
                    "appointment_id": appointment_id,
                    "date": date,
                    "time": new_slot,
                    "message": "Appointment updated successfully"
                }
            else:
                return {
                    "success": False,
                    "message": "Failed to update appointment"
                }

        except Exception as e:
            logger.error(f"Error updating appointment: {e}")
            return {
                "success": False,
                "message": f"Error updating appointment: {e}"
            }
    
    if _use_tracker:
        return await track_db_operation(
            "modify_appointment",
            _execute,
        )
    else:
        return await _execute()
