"""
Repositório Base - Classe abstrata com operações CRUD comuns.
"""
from typing import TypeVar, Generic, List, Optional, Type
from app.extensions import db

T = TypeVar('T')


class BaseRepository(Generic[T]):
    """
    Repositório base com operações CRUD genéricas.

    Uso:
        class MeuRepository(BaseRepository[MeuModel]):
            model = MeuModel
    """
    model: Type[T] = None

    @classmethod
    def get_by_id(cls, id: int) -> Optional[T]:
        """Busca registro por ID."""
        return cls.model.query.get(id)

    @classmethod
    def get_all(cls) -> List[T]:
        """Retorna todos os registros."""
        return cls.model.query.all()

    @classmethod
    def get_first(cls, **filters) -> Optional[T]:
        """Retorna o primeiro registro que corresponde aos filtros."""
        return cls.model.query.filter_by(**filters).first()

    @classmethod
    def get_many(cls, **filters) -> List[T]:
        """Retorna todos os registros que correspondem aos filtros."""
        return cls.model.query.filter_by(**filters).all()

    @classmethod
    def create(cls, **kwargs) -> T:
        """Cria um novo registro."""
        instance = cls.model(**kwargs)
        db.session.add(instance)
        db.session.commit()
        return instance

    @classmethod
    def update(cls, instance: T, **kwargs) -> T:
        """Atualiza um registro existente."""
        for key, value in kwargs.items():
            if hasattr(instance, key):
                setattr(instance, key, value)
        db.session.commit()
        return instance

    @classmethod
    def delete(cls, instance: T) -> bool:
        """Remove um registro."""
        try:
            db.session.delete(instance)
            db.session.commit()
            return True
        except Exception:
            db.session.rollback()
            return False

    @classmethod
    def save(cls, instance: T) -> T:
        """Salva (persiste) as alterações de um registro."""
        db.session.add(instance)
        db.session.commit()
        return instance

    @classmethod
    def count(cls, **filters) -> int:
        """Conta registros que correspondem aos filtros."""
        if filters:
            return cls.model.query.filter_by(**filters).count()
        return cls.model.query.count()

    @classmethod
    def exists(cls, **filters) -> bool:
        """Verifica se existe algum registro com os filtros."""
        return cls.model.query.filter_by(**filters).first() is not None

    @classmethod
    def paginate(cls, page: int = 1, per_page: int = 20, **filters):
        """Retorna registros paginados."""
        query = cls.model.query
        if filters:
            query = query.filter_by(**filters)
        return query.paginate(page=page, per_page=per_page, error_out=False)
