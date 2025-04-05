from enum import StrEnum
from typing import Any, Dict, List, Tuple, Union, Optional

class FilterOperator(StrEnum):
    """Enum of supported filter operators for SQLite queries.
    
    These match the Supabase/PostgREST filter operators for compatibility.
    """
    # Equality operators
    EQ = "eq"       # Equal to
    NEQ = "neq"     # Not equal to
    GT = "gt"       # Greater than
    GTE = "gte"     # Greater than or equal to
    LT = "lt"       # Less than
    LTE = "lte"     # Less than or equal to
    IS = "is"       # Is (for NULL, TRUE, FALSE)
    
    # Text search operators
    LIKE = "like"   # LIKE pattern matching
    ILIKE = "ilike" # Case-insensitive LIKE (implemented as LIKE COLLATE NOCASE in SQLite)
    
    # Array operators
    IN = "in"       # IN a list of values
    
    # Negation prefix
    NOT = "not"     # Negates the operator that follows

class SQLOperatorMap:
    """Maps filter operators to their SQLite SQL equivalents."""
    
    # Basic mapping of operators to SQL syntax
    OPERATORS = {
        FilterOperator.EQ: "=",
        FilterOperator.NEQ: "!=",
        FilterOperator.GT: ">",
        FilterOperator.GTE: ">=",
        FilterOperator.LT: "<",
        FilterOperator.LTE: "<=",
        FilterOperator.IS: "IS",
        FilterOperator.LIKE: "LIKE",
        FilterOperator.ILIKE: "LIKE COLLATE NOCASE",  # SQLite implementation of case-insensitive LIKE
        FilterOperator.IN: "IN",
    }
    
    @staticmethod
    def get_sql_operator(operator: str) -> str:
        """Get the SQL operator string for a given filter operator.
        
        Args:
            operator: The filter operator (e.g., "eq", "gt", "like")
            
        Returns:
            The SQL operator string
            
        Raises:
            ValueError: If the operator is not supported
        """
        try:
            # Convert string to enum if needed
            if isinstance(operator, str):
                operator = FilterOperator(operator)
                
            return SQLOperatorMap.OPERATORS[operator]
        except (KeyError, ValueError):
            raise ValueError(f"Unsupported filter operator: {operator}")
    
    @staticmethod
    def is_negated(operator: str) -> Tuple[bool, str]:
        """Check if an operator is negated and return the base operator.
        
        Args:
            operator: The filter operator, possibly with 'not.' prefix
            
        Returns:
            Tuple of (is_negated, base_operator)
        """
        if isinstance(operator, str) and operator.startswith(f"{FilterOperator.NOT}."):
            return True, operator[len(f"{FilterOperator.NOT}."):]
        return False, operator
