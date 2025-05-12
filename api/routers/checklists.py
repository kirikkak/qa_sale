from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

from api.dependencies import get_db, get_current_user, get_active_checklist
from models import Checklist as ChecklistModel
from api.schemas import ChecklistCreate, ChecklistRead, ChecklistUpdate

router = APIRouter()


@router.post("/", response_model=ChecklistRead, status_code=status.HTTP_201_CREATED)
def create_checklist(
        checklist: ChecklistCreate,
        db: Session = Depends(get_db),
        current_user=Depends(get_current_user)
):
    # Пронумеровываем элементы
    items = [{"step": i + 1, "value": item.value} for i, item in enumerate(checklist.items)]

    new = ChecklistModel(
        user_id=current_user.id,
        name=checklist.name,
        items=items,
        is_active=True,
        sort_order=checklist.sort_order
    )
    db.add(new)
    db.commit()
    db.refresh(new)
    return new

@router.get("/", response_model=List[ChecklistRead])
def list_checklists(
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    return (
        db.query(ChecklistModel)
        .filter(ChecklistModel.user_id == current_user.id, ChecklistModel.is_active == True)
        .order_by(ChecklistModel.sort_order)
        .all()
    )

@router.get("/{checklist_id}", response_model=ChecklistRead)
def get_checklist(
    checklist_id: int,
    db: Session = Depends(get_db),
    current_user=Depends(get_current_user)
):
    return get_active_checklist(checklist_id, db, current_user)


@router.put("/{checklist_id}", response_model=ChecklistRead)
def update_checklist(
        checklist_id: int,
        update: ChecklistUpdate,
        db: Session = Depends(get_db),
        current_user=Depends(get_current_user)
):
    return get_active_checklist(checklist_id, db, current_user)

    # Обновляем только переданные поля
    if update.name is not None:
        item.name = update.name
    if update.items is not None:
        # Пронумеровываем элементы
        item.items = [{"step": i + 1, "value": step.value} for i, step in enumerate(update.items)]
    if update.is_active is not None:
        item.is_active = update.is_active
    if update.sort_order is not None:
        item.sort_order = update.sort_order

    db.commit()
    db.refresh(item)
    return item


@router.delete("/{checklist_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_checklist(
        checklist_id: int,
        db: Session = Depends(get_db),
        current_user=Depends(get_current_user)
):
    # Проверяем, что чеклист активен
    item = get_active_checklist(checklist_id, db, current_user)

    # Деактивируем чеклист вместо удаления
    item.is_active = False
    db.commit()
    return None

