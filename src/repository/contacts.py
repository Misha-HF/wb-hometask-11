from sqlalchemy import or_
from sqlalchemy.orm import Session
from src import schemas
from src.database.models import Contact
from datetime import date
from sqlalchemy import select
from sqlalchemy import func


class ResponseValidationError(Exception):
    pass


async def get_contact(db: Session, contact_id: int):
    result = db.execute(select(Contact).filter(Contact.id == contact_id))
    return result.scalars().first()


async def get_contact_by_email(db: Session, email: str):
    result = db.execute(select(Contact).filter(Contact.email == email))
    return result.scalars().first()


async def get_contact_by_phone(db: Session, phone_number: str):
    result = db.execute(select(Contact).filter(Contact.phone_number == phone_number))
    return result.scalars().first()


async def get_contacts(db: Session, skip: int = 0, limit: int = 100):
    result = db.execute(select(Contact).offset(skip).limit(limit))
    return result.scalars().all()


async def create_contact(db: Session, contact: schemas.ContactCreate):
    db_contact = Contact(**contact.dict())
    db.add(db_contact)
    db.commit()
    db.refresh(db_contact)
    return db_contact


async def update_contact(db: Session, db_contact: Contact, contact: schemas.ContactUpdate):
    if contact.first_name:
        db_contact.first_name = contact.first_name
    if contact.last_name:
        db_contact.last_name = contact.last_name
    if contact.email:
        db_contact.email = contact.email
    if contact.phone_number:
        db_contact.phone_number = contact.phone_number
    if contact.birthday:
        db_contact.birthday = contact.birthday
    db.commit()
    db.refresh(db_contact)
    return db_contact


async def delete_contact(db: Session, contact_id: int):
    db_contact = await get_contact(db, contact_id)
    db.delete(db_contact)
    db.commit()
    return db_contact


async def search_contacts(db: Session, first_name: str = None, last_name: str = None, email: str = None):
    conditions = []
    if first_name:
        conditions.append(Contact.first_name == first_name)
    if last_name:
        conditions.append(Contact.last_name == last_name)
    if email:
        conditions.append(Contact.email == email)
    if not conditions:
        raise ResponseValidationError("Please provide at least one search condition.")
    query = select(Contact).filter(or_(*conditions))
    result = db.execute(query)
    return result.scalars().all()


async def get_contacts_by_birthday(db: Session, start_date: date, end_date: date):
    query = select(Contact).where(
        func.date_part('month', Contact.birthday).between(start_date.month, end_date.month) &
        func.date_part('day', Contact.birthday).between(start_date.day, end_date.day)
    )
    result = db.execute(query)
    return result.scalars().all()
