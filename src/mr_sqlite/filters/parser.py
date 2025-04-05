from typing import Dict, List, Any, Tuple, Optional, Union
import re
from .operators import FilterOperator, SQLOperatorMap

class FilterParser:
    """Parser for Supabase-style filter expressions.
    
    Converts filter expressions in the format "column.operator.value" to SQL WHERE clauses.
    """
    
    @staticmethod
    def parse_filter(raw_filter: str) -> Tuple[str, List[Any]]:
        """Parse a single filter expression in the format 'column.operator.value'.
        
        Args:
            raw_filter: Filter expression (e.g., "name.eq.John" or "age.gt.25")
            
        Returns:
            Tuple of (sql_condition, params)
            
        Raises:
            ValueError: If the filter expression is invalid
        """
        parts = raw_filter.strip().split('.', 2)
        if len(parts) < 3:
            raise ValueError(f"Invalid filter format: {raw_filter}. Expected format: column.operator.value")
            
        column, operator, value = parts
        
        # Check for negation
        is_negated, base_operator = SQLOperatorMap.is_negated(operator)
        
        # Get SQL operator
        try:
            sql_operator = SQLOperatorMap.get_sql_operator(base_operator)
        except ValueError as e:
            raise ValueError(f"Invalid operator in filter '{raw_filter}': {str(e)}")
        
        # Handle special cases
        if base_operator == FilterOperator.IN:
            # Split comma-separated values for IN operator
            values = value.split(',')
            placeholders = ','.join(['?'] * len(values))
            condition = f"{column} {'' if not is_negated else 'NOT '}{sql_operator} ({placeholders})"
            return condition, values
        elif base_operator == FilterOperator.IS and value.lower() in ('null', 'true', 'false'):
            # Special handling for IS NULL, IS TRUE, IS FALSE
            value_upper = value.upper()
            condition = f"{column} {'' if not is_negated else 'NOT '}{sql_operator} {value_upper}"
            return condition, []
        else:
            # Standard case with a single parameter
            # Convert value if needed
            param_value = FilterParser._convert_value(value)
            
            # For ILIKE, we use LIKE with COLLATE NOCASE in SQLite
            if base_operator == FilterOperator.ILIKE:
                condition = f"{column} {'' if not is_negated else 'NOT '}{sql_operator} ?"
            else:
                condition = f"{column} {'' if not is_negated else 'NOT '}{sql_operator} ?"
                
            return condition, [param_value]
    
    @staticmethod
    def parse_raw_filters(raw_filters: str) -> Tuple[str, List[Any]]:
        """Parse a comma-separated list of filter expressions.
        
        Args:
            raw_filters: Comma-separated filter expressions
                        (e.g., "name.eq.John,age.gt.25")
            
        Returns:
            Tuple of (where_clause, params)
        """
        if not raw_filters:
            return "", []
            
        filter_expressions = raw_filters.split(',')
        conditions = []
        all_params = []
        
        for expr in filter_expressions:
            try:
                condition, params = FilterParser.parse_filter(expr)
                conditions.append(condition)
                all_params.extend(params)
            except ValueError as e:
                # Log the error but continue with other filters
                print(f"Warning: {str(e)}")
                continue
            
        if not conditions:
            return "", []
            
        where_clause = " AND ".join(conditions)
        return where_clause, all_params
    
    @staticmethod
    def _convert_value(value: str) -> Any:
        """Convert a string value to the appropriate Python type.
        
        Args:
            value: String value from filter expression
            
        Returns:
            Converted value (None, bool, int, float, or string)
        """
        # Handle special values
        if value.lower() == 'null':
            return None
        elif value.lower() == 'true':
            return True
        elif value.lower() == 'false':
            return False
            
        # Try to convert to number
        if value.isdigit():
            return int(value)
        
        # Try to convert to float
        try:
            return float(value)
        except ValueError:
            pass
            
        # Default to string
        return value
