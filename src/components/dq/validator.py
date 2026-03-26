from pyspark.sql import DataFrame
import logging

class DataQualityValidator:
    """
    Standardized Data Quality Framework for Medallion Architecture.
    Inspired by Great Expectations principles.
    """
    def __init__(self, df: DataFrame, table_name: str):
        self.df = df
        self.table_name = table_name
        self.results = []

    def expect_column_values_to_not_be_null(self, column: str, threshold: float = 0.0):
        """Valida que el porcentaje de nulos esté por debajo del umbral."""
        total_count = self.df.count()
        if total_count == 0:
            return self
            
        null_count = self.df.filter(self.df[column].isNull()).count()
        null_pct = null_count / total_count
        
        status = "PASS" if null_pct <= threshold else "FAIL"
        self.results.append({
            "expectation": "not_be_null",
            "column": column,
            "status": status,
            "observed_value": f"{null_pct:.2%}",
            "threshold": f"{threshold:.2%}"
        })
        return self

    def expect_column_values_to_be_unique(self, column: str):
        """Valida que no haya duplicados en una columna clave."""
        total_count = self.df.count()
        unique_count = self.df.select(column).distinct().count()
        
        status = "PASS" if total_count == unique_count else "FAIL"
        self.results.append({
            "expectation": "be_unique",
            "column": column,
            "status": status,
            "observed_value": f"Duplicates: {total_count - unique_count}",
            "threshold": "0"
        })
        return self

    def expect_column_values_to_be_between(self, column: str, min_val: float, max_val: float):
        """Valida que los valores de la columna estén en el rango [min, max]."""
        total_count = self.df.count()
        if total_count == 0:
            return self

        out_of_range = self.df.filter((self.df[column] < min_val) | (self.df[column] > max_val)).count()
        
        status = "PASS" if out_of_range == 0 else "FAIL"
        self.results.append({
            "expectation": "be_between",
            "column": column,
            "status": status,
            "observed_value": f"Out of range: {out_of_range}",
            "threshold": f"[{min_val}, {max_val}]"
        })
        return self

    def expect_column_values_to_exist_in_table(self, column: str, parent_df: DataFrame, parent_column: str):
        """Valida Integridad Referencial (FK -> PK)."""
        orphans = self.df.join(parent_df, self.df[column] == parent_df[parent_column], "left_anti").count()
        status = "PASS" if orphans == 0 else "FAIL"
        self.results.append({
            "expectation": "referential_integrity",
            "column": column,
            "status": status,
            "observed_value": f"Orphans: {orphans}",
            "threshold": "0"
        })
        return self

    def validate(self, halt_on_fail: bool = False):
        """Ejecuta y reporta los resultados."""
        print(f"--- DQ REPORT: {self.table_name} ---")
        failed = False
        for res in self.results:
            icon = "✅" if res["status"] == "PASS" else "❌"
            print(f"{icon} {res['expectation']} on '{res['column']}': {res['status']} (Observed: {res['observed_value']}, Target: {res['threshold']})")
            if res["status"] == "FAIL":
                failed = True
        
        if failed and halt_on_fail:
            raise Exception(f"Critical DQ Failure in {self.table_name}. Pipeline halted.")
        
        return not failed
