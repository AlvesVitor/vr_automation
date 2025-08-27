import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
from pathlib import Path
import warnings
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum

warnings.filterwarnings('ignore')


class ValidationLevel(Enum):
    """Níveis de validação do sistema."""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


@dataclass
class ValidationResult:
    """Resultado de uma validação específica."""
    level: ValidationLevel
    message: str
    count: int = 0
    details: Optional[str] = None


@dataclass
class ProcessingStats:
    """Estatísticas do processamento."""
    total_collaborators: int
    total_days: int
    total_value: float
    company_cost: float
    professional_discount: float
    excluded_count: int
    processing_time: float


class DataLoader:
    """Classe responsável pelo carregamento de dados."""
    
    FILE_MAPPING = {
        'admissao': 'ADMISSÃO ABRIL.xlsx',
        'afastamentos': 'AFASTAMENTOS.xlsx',
        'aprendiz': 'APRENDIZ.xlsx',
        'ativos': 'ATIVOS.xlsx',
        'dias_uteis': 'Base dias uteis.xlsx',
        'sindicato_valor': 'Base sindicato x valor.xlsx',
        'desligados': 'DESLIGADOS.xlsx',
        'estagio': 'ESTÁGIO.xlsx',
        'exterior': 'EXTERIOR.xlsx',
        'ferias': 'FÉRIAS.xlsx'
    }
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.loaded_data = {}
        self.failed_files = []
        
    def load_all_data(self, files_folder: str) -> Dict[str, pd.DataFrame]:
        """Carrega todas as bases de dados necessárias."""
        self.logger.info("📁 Iniciando carregamento das bases de dados...")
        
        files_folder = Path(files_folder)
        
        if not files_folder.exists():
            raise FileNotFoundError(f"Pasta de dados não encontrada: {files_folder}")
        
        for name, filename in self.FILE_MAPPING.items():
            file_path = files_folder / filename
            
            try:
                if file_path.exists():
                    df = pd.read_excel(file_path)
                    self.loaded_data[name] = df
                    self.logger.info(f"  ✅ {filename}: {len(df)} registros carregados")
                else:
                    self.logger.warning(f"  ⚠️  {filename}: arquivo não encontrado")
                    self.failed_files.append(filename)
                    
            except Exception as e:
                self.logger.error(f"  ❌ Erro ao carregar {filename}: {str(e)}")
                self.failed_files.append(filename)
        
        self._validate_essential_files()
        return self.loaded_data
    
    def _validate_essential_files(self) -> None:
        """Valida se arquivos essenciais foram carregados."""
        essential_files = ['ativos', 'sindicato_valor', 'dias_uteis']
        missing_essential = [f for f in essential_files if f not in self.loaded_data]
        
        if missing_essential:
            raise ValueError(f"Arquivos essenciais não encontrados: {missing_essential}")


class DataProcessor:
    """Classe responsável pelo processamento e limpeza de dados."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        
    def clean_and_standardize(self, data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Limpa e padroniza os dados das bases."""
        self.logger.info("🧹 Iniciando limpeza e padronização dos dados...")
        
        # Padronizar colunas MATRICULA
        matricula_files = ['admissao', 'afastamentos', 'aprendiz', 'ativos', 'desligados', 'estagio', 'ferias']
        
        for name in matricula_files:
            if name in data and 'MATRICULA' in data[name].columns:
                data[name]['MATRICULA'] = (
                    data[name]['MATRICULA']
                    .astype(str)
                    .str.strip()
                    .str.upper()
                )
        
        # Tratar arquivo exterior (coluna diferente)
        if 'exterior' in data and 'MATRICULA' in data['exterior'].columns:
            data['exterior']['MATRICULA'] = (
                data['exterior']['MATRICULA']
                .astype(str)
                .str.strip()
                .str.upper()
            )
        
        # Converter e validar datas
        self._process_dates(data)
        
        # Padronizar nomes de sindicatos
        self._standardize_union_names(data)
        
        self.logger.info("  ✅ Dados limpos e padronizados com sucesso")
        return data
    
    def _process_dates(self, data: Dict[str, pd.DataFrame]) -> None:
        """Processa e valida campos de data."""
        date_conversions = [
            ('admissao', 'Admissão'),
            ('desligados', 'DATA DEMISSÃO')
        ]
        
        for file_key, column_name in date_conversions:
            if file_key in data and column_name in data[file_key].columns:
                original_count = len(data[file_key])
                data[file_key][column_name] = pd.to_datetime(
                    data[file_key][column_name], 
                    errors='coerce',
                    dayfirst=True
                )
                
                invalid_dates = data[file_key][column_name].isna().sum()
                if invalid_dates > 0:
                    self.logger.warning(
                        f"  ⚠️  {file_key}.{column_name}: {invalid_dates}/{original_count} datas inválidas"
                    )
    
    def _standardize_union_names(self, data: Dict[str, pd.DataFrame]) -> None:
        """Padroniza nomes de sindicatos."""
        union_files = ['ativos']
        
        for file_key in union_files:
            if file_key in data and 'SINDICATO' in data[file_key].columns:
                data[file_key]['SINDICATO'] = (
                    data[file_key]['SINDICATO']
                    .astype(str)
                    .str.strip()
                    .str.upper()
                )


class ExclusionManager:
    """Gerencia exclusões de colaboradores."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.exclusion_details = {}
        
    def identify_exclusions(self, data: Dict[str, pd.DataFrame]) -> set:
        """Identifica todas as matrículas a serem excluídas."""
        self.logger.info("🚫 Identificando exclusões por categoria...")
        
        all_exclusions = set()
        
        exclusion_rules = [
            ('directors', self._get_directors),
            ('interns', self._get_interns),
            ('apprentices', self._get_apprentices),
            ('away', self._get_away),
            ('exterior', self._get_exterior)
        ]
        
        for category, rule_func in exclusion_rules:
            excluded = rule_func(data)
            self.exclusion_details[category] = excluded
            all_exclusions.update(excluded)
            self.logger.info(f"  📊 {category.title()}: {len(excluded)} exclusões")
        
        self.logger.info(f"  🎯 Total de exclusões únicas: {len(all_exclusions)}")
        return all_exclusions
    
    def _get_directors(self, data: Dict[str, pd.DataFrame]) -> set:
        """Identifica diretores pelo cargo."""
        if 'ativos' not in data:
            return set()
            
        directors_mask = (
            data['ativos']['TITULO DO CARGO']
            .str.contains('DIRETOR', case=False, na=False)
        )
        return set(data['ativos'][directors_mask]['MATRICULA'].tolist())
    
    def _get_interns(self, data: Dict[str, pd.DataFrame]) -> set:
        """Identifica estagiários."""
        if 'estagio' not in data:
            return set()
        return set(data['estagio']['MATRICULA'].tolist())
    
    def _get_apprentices(self, data: Dict[str, pd.DataFrame]) -> set:
        """Identifica aprendizes."""
        if 'aprendiz' not in data:
            return set()
        return set(data['aprendiz']['MATRICULA'].tolist())
    
    def _get_away(self, data: Dict[str, pd.DataFrame]) -> set:
        """Identifica afastados."""
        if 'afastamentos' not in data:
            return set()
        return set(data['afastamentos']['MATRICULA'].tolist())
    
    def _get_exterior(self, data: Dict[str, pd.DataFrame]) -> set:
        """Identifica colaboradores no exterior."""
        if 'exterior' not in data:
            return set()
        return set(data['exterior']['MATRICULA'].tolist())


class CalculationEngine:
    """Motor de cálculos para VR."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.business_days_mapping = {}
        self.daily_values_mapping = {}
        
    def prepare_calculation_data(self, data: Dict[str, pd.DataFrame]) -> None:
        """Prepara dados para cálculos."""
        self._build_business_days_mapping(data)
        self._build_daily_values_mapping(data)
    
    def _build_business_days_mapping(self, data: Dict[str, pd.DataFrame]) -> None:
        """Constrói mapeamento de dias úteis por sindicato."""
        if 'dias_uteis' not in data:
            self.logger.warning("  ⚠️  Base de dias úteis não encontrada, usando padrão de 22 dias")
            return
            
        for _, row in data['dias_uteis'].iterrows():
            union = str(row['SINDICATO']).strip().upper()
            days = int(row['DIAS UTEIS']) if pd.notna(row['DIAS UTEIS']) else 22
            self.business_days_mapping[union] = days
            
        self.logger.info(f"  📊 Mapeamento de dias úteis: {len(self.business_days_mapping)} sindicatos")
    
    def _build_daily_values_mapping(self, data: Dict[str, pd.DataFrame]) -> None:
        """Constrói mapeamento de valores diários por estado."""
        if 'sindicato_valor' not in data:
            self.logger.warning("  ⚠️  Base de valores não encontrada, usando padrão R$ 35,00")
            return
            
        for _, row in data['sindicato_valor'].iterrows():
            state = str(row['ESTADO']).strip().upper()
            value_str = str(row['VALOR']).replace('R$', '').replace(' ', '').replace(',', '.')
            
            try:
                value = float(value_str)
                self.daily_values_mapping[state] = value
            except ValueError:
                self.logger.warning(f"  ⚠️  Valor inválido para estado {state}: {row['VALOR']}")
                
        self.logger.info(f"  📊 Mapeamento de valores: {len(self.daily_values_mapping)} estados")
    
    def calculate_days_worked(self, 
                            base_days: int,
                            vacation_days: int,
                            admission_date: Optional[datetime],
                            dismissal_date: Optional[datetime],
                            competence_month: int,
                            competence_year: int) -> int:
        """Calcula dias trabalhados considerando todas as regras."""
        
        days_worked = base_days - vacation_days
        
        # Ajuste para admissão no meio do mês
        if (admission_date and 
            admission_date.month == competence_month and 
            admission_date.year == competence_year):
            
            remaining_days = 30 - admission_date.day + 1
            proportion = remaining_days / 30
            days_worked = int(base_days * proportion)
        
        # Ajuste para demissão proporcional
        if (dismissal_date and 
            dismissal_date.month == competence_month and 
            dismissal_date.year == competence_year):
            
            worked_days_month = dismissal_date.day
            proportion = worked_days_month / 30
            days_worked = int(base_days * proportion)
        
        return max(0, days_worked)
    
    def get_daily_value(self, union_name: str) -> float:
        """Obtém valor diário baseado no sindicato/estado."""
        union_upper = union_name.upper()
        
        for state, value in self.daily_values_mapping.items():
            if state in union_upper:
                return value
                
        return 35.0  # Valor padrão


class DataValidator:
    """Validador de dados processados."""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        
    def validate_processed_data(self, df: pd.DataFrame) -> List[ValidationResult]:
        """Executa todas as validações nos dados processados."""
        self.logger.info("🔍 Executando validações dos dados processados...")
        
        validations = []
        
        # Validação 1: Dias negativos
        validations.append(self._validate_negative_days(df))
        
        # Validação 2: Valores inconsistentes
        validations.append(self._validate_value_consistency(df))
        
        # Validação 3: Rateio empresa/profissional
        validations.append(self._validate_cost_split(df))
        
        # Validação 4: Matrículas duplicadas
        validations.append(self._validate_duplicate_registrations(df))
        
        # Validação 5: Valores extremos
        validations.append(self._validate_extreme_values(df))
        
        # Validação 6: Dados obrigatórios
        validations.append(self._validate_required_fields(df))
        
        return validations
    
    def _validate_negative_days(self, df: pd.DataFrame) -> ValidationResult:
        """Valida se há dias negativos."""
        negative_count = len(df[df['Dias'] < 0])
        
        if negative_count > 0:
            return ValidationResult(
                level=ValidationLevel.ERROR,
                message="Registros com dias negativos encontrados",
                count=negative_count,
                details="Verifique cálculos de férias e admissões"
            )
        
        return ValidationResult(
            level=ValidationLevel.INFO,
            message="Todos os registros têm dias válidos",
            count=0
        )
    
    def _validate_value_consistency(self, df: pd.DataFrame) -> ValidationResult:
        """Valida consistência entre dias e valores."""
        inconsistent = df[
            (df['Dias'] > 0) & (df['TOTAL'] <= 0)
        ]
        count = len(inconsistent)
        
        if count > 0:
            return ValidationResult(
                level=ValidationLevel.ERROR,
                message="Registros com dias > 0 mas valor = 0",
                count=count,
                details="Verifique mapeamento de valores por estado"
            )
        
        return ValidationResult(
            level=ValidationLevel.INFO,
            message="Valores consistentes com dias trabalhados",
            count=0
        )
    
    def _validate_cost_split(self, df: pd.DataFrame) -> ValidationResult:
        """Valida se o rateio empresa/profissional está correto."""
        tolerance = 0.01
        incorrect_split = df[
            abs(df['Custo empresa'] + df['Desconto profissional'] - df['TOTAL']) > tolerance
        ]
        count = len(incorrect_split)
        
        if count > 0:
            return ValidationResult(
                level=ValidationLevel.ERROR,
                message="Divergências no rateio empresa/profissional",
                count=count,
                details="Soma dos valores não corresponde ao total"
            )
        
        return ValidationResult(
            level=ValidationLevel.INFO,
            message="Rateio empresa/profissional correto",
            count=0
        )
    
    def _validate_duplicate_registrations(self, df: pd.DataFrame) -> ValidationResult:
        """Valida matrículas duplicadas."""
        total_records = len(df)
        unique_records = df['Matricula'].nunique()
        duplicates = total_records - unique_records
        
        if duplicates > 0:
            return ValidationResult(
                level=ValidationLevel.ERROR,
                message="Matrículas duplicadas encontradas",
                count=duplicates,
                details="Verifique processamento de dados base"
            )
        
        return ValidationResult(
            level=ValidationLevel.INFO,
            message="Todas as matrículas são únicas",
            count=0
        )
    
    def _validate_extreme_values(self, df: pd.DataFrame) -> ValidationResult:
        """Valida valores extremos (muito altos ou baixos)."""
        # Considera valores extremos: > R$ 3000 ou dias > 30
        extreme_values = df[
            (df['TOTAL'] > 3000) | (df['Dias'] > 30)
        ]
        count = len(extreme_values)
        
        if count > 0:
            return ValidationResult(
                level=ValidationLevel.WARNING,
                message="Valores extremos detectados",
                count=count,
                details="Verifique se os cálculos estão corretos"
            )
        
        return ValidationResult(
            level=ValidationLevel.INFO,
            message="Valores dentro da faixa esperada",
            count=0
        )
    
    def _validate_required_fields(self, df: pd.DataFrame) -> ValidationResult:
        """Valida campos obrigatórios."""
        required_fields = ['Matricula', 'Sindicato do Colaborador', 'Competência']
        
        missing_data = 0
        for field in required_fields:
            if field in df.columns:
                missing_data += df[field].isna().sum()
        
        if missing_data > 0:
            return ValidationResult(
                level=ValidationLevel.WARNING,
                message="Dados obrigatórios ausentes",
                count=missing_data,
                details=f"Campos verificados: {', '.join(required_fields)}"
            )
        
        return ValidationResult(
            level=ValidationLevel.INFO,
            message="Todos os campos obrigatórios preenchidos",
            count=0
        )


class VRAutomation:
    """Classe principal do sistema de automação VR."""
    
    def __init__(self):
        self.logger = self._setup_logger()
        self.data_loader = DataLoader(self.logger)
        self.data_processor = DataProcessor(self.logger)
        self.exclusion_manager = ExclusionManager(self.logger)
        self.calculation_engine = CalculationEngine(self.logger)
        self.validator = DataValidator(self.logger)
        
        self.raw_data = {}
        self.processed_data = {}
        self.final_result = None
        self.processing_stats = None
        
    def _setup_logger(self) -> logging.Logger:
        """Configura logger específico para VR Automation."""
        logger = logging.getLogger('VRAutomation')
        
        if not logger.handlers:  # Evita duplicação de handlers
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
            
        return logger
    
    def load_data(self, files_folder: str) -> Dict[str, pd.DataFrame]:
        """Carrega todas as bases de dados."""
        self.raw_data = self.data_loader.load_all_data(files_folder)
        return self.raw_data
    
    def process_data(self) -> Dict[str, pd.DataFrame]:
        """Processa e limpa os dados carregados."""
        if not self.raw_data:
            raise ValueError("Dados não carregados. Execute load_data() primeiro.")
            
        self.processed_data = self.data_processor.clean_and_standardize(self.raw_data)
        return self.processed_data
    
    def create_consolidated_base(self, competence_month: int = 5, competence_year: int = 2025) -> pd.DataFrame:
        """Cria a base consolidada final com todos os cálculos."""
        start_time = datetime.now()
        
        self.logger.info(f"🔄 Criando base consolidada para {competence_month:02d}/{competence_year}...")
        
        if not self.processed_data:
            raise ValueError("Dados não processados. Execute process_data() primeiro.")
        
        # Preparar dados de cálculo
        self.calculation_engine.prepare_calculation_data(self.processed_data)
        
        # Base principal: colaboradores ativos
        if 'ativos' not in self.processed_data:
            raise ValueError("Base de colaboradores ativos não encontrada!")
        
        base_final = self.processed_data['ativos'][
            ['MATRICULA', 'TITULO DO CARGO', 'DESC. SITUACAO', 'SINDICATO']
        ].copy()
        
        # Adicionar informações complementares
        base_final = self._enrich_base_data(base_final)
        
        # Aplicar exclusões
        exclusions = self.exclusion_manager.identify_exclusions(self.processed_data)
        initial_count = len(base_final)
        base_final = base_final[~base_final['MATRICULA'].isin(exclusions)]
        excluded_count = initial_count - len(base_final)
        
        self.logger.info(f"  📊 Após exclusões: {len(base_final)} colaboradores elegíveis")
        
        # Aplicar regras de elegibilidade
        base_final = self._apply_eligibility_rules(base_final)
        
        # Gerar registros finais com cálculos
        results = self._generate_final_records(base_final, competence_month, competence_year)
        
        self.final_result = pd.DataFrame(results)
        
        # Calcular estatísticas
        processing_time = (datetime.now() - start_time).total_seconds()
        self.processing_stats = ProcessingStats(
            total_collaborators=len(self.final_result),
            total_days=self.final_result['Dias'].sum(),
            total_value=self.final_result['TOTAL'].sum(),
            company_cost=self.final_result['Custo empresa'].sum(),
            professional_discount=self.final_result['Desconto profissional'].sum(),
            excluded_count=excluded_count,
            processing_time=processing_time
        )
        
        self.logger.info(f"  ✅ Base consolidada criada: {len(self.final_result)} registros")
        self.logger.info(f"  ⏱️  Tempo de processamento: {processing_time:.2f}s")
        
        return self.final_result
    
    def _enrich_base_data(self, base_df: pd.DataFrame) -> pd.DataFrame:
        """Enriquece dados base com informações complementares."""
        # Adicionar admissões
        if 'admissao' in self.processed_data:
            admissions = self.processed_data['admissao'][['MATRICULA', 'Admissão']].copy()
            base_df = base_df.merge(admissions, on='MATRICULA', how='left')
        
        # Adicionar férias
        base_df['Dias_Ferias'] = 0
        if 'ferias' in self.processed_data:
            holidays_dict = dict(zip(
                self.processed_data['ferias']['MATRICULA'], 
                self.processed_data['ferias']['DIAS DE FÉRIAS'].fillna(0)
            ))
            base_df['Dias_Ferias'] = base_df['MATRICULA'].map(holidays_dict).fillna(0)
        
        # Adicionar informações de desligamento
        base_df['Data_Demissao'] = None
        base_df['Comunicado_Desligamento'] = None
        
        if 'desligados' in self.processed_data:
            disconnected_info = self.processed_data['desligados'].set_index('MATRICULA')
            
            for idx, row in base_df.iterrows():
                matricula = row['MATRICULA']
                if matricula in disconnected_info.index:
                    base_df.at[idx, 'Data_Demissao'] = disconnected_info.loc[matricula, 'DATA DEMISSÃO']
                    base_df.at[idx, 'Comunicado_Desligamento'] = disconnected_info.loc[matricula, 'COMUNICADO DE DESLIGAMENTO']
        
        return base_df
    
    def _apply_eligibility_rules(self, base_df: pd.DataFrame) -> pd.DataFrame:
        """Aplica regras de elegibilidade para pagamento."""
        base_df['Elegivel_Pagamento'] = True
        
        # Regra de desligamento: se desligado até dia 15 com comunicado OK, não paga
        mask_dismissal = (
            (base_df['Comunicado_Desligamento'] == 'OK') &
            (pd.notna(base_df['Data_Demissao'])) &
            (base_df['Data_Demissao'].dt.day <= 15)
        )
        
        base_df.loc[mask_dismissal, 'Elegivel_Pagamento'] = False
        ineligible_count = mask_dismissal.sum()
        
        if ineligible_count > 0:
            self.logger.info(f"  📊 Colaboradores inelegíveis por regra de desligamento: {ineligible_count}")
        
        return base_df[base_df['Elegivel_Pagamento'] == True]
    
    def _generate_final_records(self, base_df: pd.DataFrame, competence_month: int, competence_year: int) -> List[Dict]:
        """Gera registros finais com todos os cálculos."""
        results = []
        
        for _, employee in base_df.iterrows():
            # Dados básicos
            registration = employee['MATRICULA']
            union = employee['SINDICATO']
            admission_date = employee.get('Admissão')
            vacation_days = int(employee['Dias_Ferias'])
            dismissal_date = employee.get('Data_Demissao')
            
            # Obter dias úteis para o sindicato
            base_days = self.calculation_engine.business_days_mapping.get(union, 22)
            
            # Calcular dias trabalhados
            days_worked = self.calculation_engine.calculate_days_worked(
                base_days=base_days,
                vacation_days=vacation_days,
                admission_date=admission_date,
                dismissal_date=dismissal_date,
                competence_month=competence_month,
                competence_year=competence_year
            )
            
            # Obter valor diário
            daily_value = self.calculation_engine.get_daily_value(union)
            
            # Calcular valores finais
            total_value = days_worked * daily_value
            company_cost = total_value * 0.8
            professional_discount = total_value * 0.2
            
            # Criar registro
            result = {
                'Matricula': registration,
                'Admissão': admission_date.strftime('%d/%m/%Y') if pd.notna(admission_date) else '',
                'Sindicato do Colaborador': union,
                'Competência': f'01/{competence_month:02d}/{competence_year}',
                'Dias': days_worked,
                'VALOR DIÁRIO VR': daily_value,
                'TOTAL': round(total_value, 2),
                'Custo empresa': round(company_cost, 2),
                'Desconto profissional': round(professional_discount, 2),
                'OBS GERAL': self._generate_observations(employee, vacation_days, admission_date, dismissal_date)
            }
            
            results.append(result)
        
        return results
    
    def _generate_observations(self, employee: pd.Series, vacation_days: int, 
                             admission_date: Optional[datetime], dismissal_date: Optional[datetime]) -> str:
        """Gera observações para o registro."""
        observations = []
        
        if vacation_days > 0:
            observations.append(f"Férias: {vacation_days} dias")
        
        if pd.notna(admission_date):
            observations.append(f"Admissão: {admission_date.strftime('%d/%m/%Y')}")
        
        if pd.notna(dismissal_date):
            observations.append(f"Demissão: {dismissal_date.strftime('%d/%m/%Y')}")
        
        return "; ".join(observations)
    
    def generate_final_report(self, output_file: str = 'VR_MENSAL_AUTOMATIZADO.xlsx') -> str:
        """Gera o arquivo final no formato esperado."""
        if self.final_result is None:
            raise ValueError("Execute create_consolidated_base() primeiro!")
        
        self.logger.info(f"📄 Gerando arquivo final: {output_file}")
        
        # Garantir que o diretório existe
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Ordenar por matrícula para melhor organização
        self.final_result = self.final_result.sort_values('Matricula')
        
        # Salvar arquivo Excel
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                self.final_result.to_excel(writer, sheet_name='VR_Consolidado', index=False)
                
                # Adicionar planilha de estatísticas se disponível
                if self.processing_stats:
                    stats_df = pd.DataFrame([{
                        'Métrica': 'Total de Colaboradores',
                        'Valor': self.processing_stats.total_collaborators
                    }, {
                        'Métrica': 'Total de Dias',
                        'Valor': self.processing_stats.total_days
                    }, {
                        'Métrica': 'Valor Total (R$)',
                        'Valor': f'{self.processing_stats.total_value:,.2f}'
                    }, {
                        'Métrica': 'Custo Empresa (R$)',
                        'Valor': f'{self.processing_stats.company_cost:,.2f}'
                    }, {
                        'Métrica': 'Desconto Profissional (R$)',
                        'Valor': f'{self.processing_stats.professional_discount:,.2f}'
                    }, {
                        'Métrica': 'Colaboradores Excluídos',
                        'Valor': self.processing_stats.excluded_count
                    }, {
                        'Métrica': 'Tempo de Processamento (s)',
                        'Valor': f'{self.processing_stats.processing_time:.2f}'
                    }])
                    
                    stats_df.to_excel(writer, sheet_name='Estatísticas', index=False)
        
        except Exception as e:
            self.logger.error(f"❌ Erro ao salvar arquivo: {e}")
            raise
        
        # Exibir estatísticas
        if self.processing_stats:
            self.logger.info("📊 Estatísticas do processamento:")
            self.logger.info(f"   • Colaboradores: {self.processing_stats.total_collaborators}")
            self.logger.info(f"   • Total de dias: {self.processing_stats.total_days}")
            self.logger.info(f"   • Valor total: R$ {self.processing_stats.total_value:,.2f}")
            self.logger.info(f"   • Custo empresa: R$ {self.processing_stats.company_cost:,.2f}")
            self.logger.info(f"   • Desconto profissional: R$ {self.processing_stats.professional_discount:,.2f}")
        
        self.logger.info(f"✅ Arquivo salvo com sucesso: {output_file}")
        return output_file
    
    def validate_data(self) -> bool:
        """Executa validações completas nos dados processados."""
        if self.final_result is None:
            self.logger.warning("❌ Nenhum dado para validar")
            return False
        
        validations = self.validator.validate_processed_data(self.final_result)
        
        # Agrupar resultados por nível
        errors = [v for v in validations if v.level == ValidationLevel.ERROR]
        warnings = [v for v in validations if v.level == ValidationLevel.WARNING]
        info = [v for v in validations if v.level == ValidationLevel.INFO]
        
        # Exibir resultados
        self.logger.info("🔍 Resultados da validação:")
        
        for validation in errors:
            self.logger.error(f"   ❌ {validation.message}: {validation.count} casos")
            if validation.details:
                self.logger.error(f"      Detalhes: {validation.details}")
        
        for validation in warnings:
            self.logger.warning(f"   ⚠️  {validation.message}: {validation.count} casos")
            if validation.details:
                self.logger.warning(f"      Detalhes: {validation.details}")
        
        for validation in info:
            self.logger.info(f"   ✅ {validation.message}")
        
        # Retornar True apenas se não houver erros
        validation_passed = len(errors) == 0
        
        if validation_passed:
            self.logger.info("✅ Todas as validações passaram com sucesso!")
        else:
            self.logger.error(f"❌ Validação falhou: {len(errors)} erros encontrados")
        
        return validation_passed
    
    def generate_summary_report(self) -> Optional[pd.DataFrame]:
        """Gera relatório resumido por sindicato."""
        if self.final_result is None:
            self.logger.warning("❌ Nenhum dado para gerar relatório")
            return None
        
        self.logger.info("📈 Gerando relatório resumido por sindicato...")
        
        try:
            summary = self.final_result.groupby('Sindicato do Colaborador').agg({
                'Matricula': 'count',
                'Dias': 'sum',
                'TOTAL': 'sum',
                'Custo empresa': 'sum',
                'Desconto profissional': 'sum',
                'VALOR DIÁRIO VR': 'mean'  # Valor médio por sindicato
            }).round(2)
            
            # Renomear colunas para melhor legibilidade
            summary.columns = [
                'Qtd_Funcionarios', 
                'Total_Dias', 
                'Valor_Total', 
                'Custo_Empresa', 
                'Desconto_Funcionario',
                'Valor_Medio_Diario'
            ]
            
            # Adicionar coluna de valor médio por funcionário
            summary['Valor_Medio_Por_Funcionario'] = (
                summary['Valor_Total'] / summary['Qtd_Funcionarios']
            ).round(2)
            
            # Ordenar por valor total decrescente
            summary = summary.sort_values('Valor_Total', ascending=False)
            
            # Exibir resumo no log
            self.logger.info("📊 Resumo por sindicato (Top 10):")
            
            for sindicato, dados in summary.head(10).iterrows():
                sindicato_short = sindicato[:40] + "..." if len(sindicato) > 40 else sindicato
                self.logger.info(f"   📋 {sindicato_short}")
                self.logger.info(f"      • Funcionários: {dados['Qtd_Funcionarios']}")
                self.logger.info(f"      • Dias: {dados['Total_Dias']}")
                self.logger.info(f"      • Valor Total: R$ {dados['Valor_Total']:,.2f}")
                self.logger.info(f"      • Valor Médio/Func: R$ {dados['Valor_Medio_Por_Funcionario']:,.2f}")
            
            if len(summary) > 10:
                self.logger.info(f"   ... e mais {len(summary) - 10} sindicatos")
            
            # Totais gerais
            total_funcionarios = summary['Qtd_Funcionarios'].sum()
            total_geral = summary['Valor_Total'].sum()
            
            self.logger.info("📈 Totais Gerais:")
            self.logger.info(f"   • Total de Funcionários: {total_funcionarios}")
            self.logger.info(f"   • Valor Total Geral: R$ {total_geral:,.2f}")
            self.logger.info(f"   • Número de Sindicatos: {len(summary)}")
            
            return summary
            
        except Exception as e:
            self.logger.error(f"❌ Erro ao gerar relatório resumido: {e}")
            return None
    
    def export_detailed_report(self, output_file: str) -> str:
        """Exporta relatório detalhado com múltiplas abas."""
        if self.final_result is None:
            raise ValueError("Execute create_consolidated_base() primeiro!")
        
        self.logger.info(f"📋 Exportando relatório detalhado: {output_file}")
        
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # Aba 1: Dados consolidados
                self.final_result.to_excel(
                    writer, 
                    sheet_name='Dados_Consolidados', 
                    index=False
                )
                
                # Aba 2: Resumo por sindicato
                summary = self.generate_summary_report()
                if summary is not None:
                    summary.to_excel(writer, sheet_name='Resumo_Sindicatos')
                
                # Aba 3: Estatísticas detalhadas
                if self.processing_stats:
                    detailed_stats = pd.DataFrame([
                        {'Categoria': 'Processamento', 'Métrica': 'Total de Colaboradores', 'Valor': self.processing_stats.total_collaborators},
                        {'Categoria': 'Processamento', 'Métrica': 'Colaboradores Excluídos', 'Valor': self.processing_stats.excluded_count},
                        {'Categoria': 'Processamento', 'Métrica': 'Tempo de Processamento (s)', 'Valor': self.processing_stats.processing_time},
                        {'Categoria': 'Dias', 'Métrica': 'Total de Dias', 'Valor': self.processing_stats.total_days},
                        {'Categoria': 'Dias', 'Métrica': 'Média de Dias por Funcionário', 'Valor': round(self.processing_stats.total_days / self.processing_stats.total_collaborators, 2)},
                        {'Categoria': 'Valores', 'Métrica': 'Valor Total (R$)', 'Valor': self.processing_stats.total_value},
                        {'Categoria': 'Valores', 'Métrica': 'Custo Empresa (R$)', 'Valor': self.processing_stats.company_cost},
                        {'Categoria': 'Valores', 'Métrica': 'Desconto Profissional (R$)', 'Valor': self.processing_stats.professional_discount},
                        {'Categoria': 'Valores', 'Métrica': 'Valor Médio por Funcionário (R$)', 'Valor': round(self.processing_stats.total_value / self.processing_stats.total_collaborators, 2)},
                    ])
                    detailed_stats.to_excel(writer, sheet_name='Estatísticas_Detalhadas', index=False)
                
                # Aba 4: Detalhes de exclusões (se disponível)
                if hasattr(self.exclusion_manager, 'exclusion_details'):
                    exclusion_data = []
                    for category, matriculas in self.exclusion_manager.exclusion_details.items():
                        for matricula in matriculas:
                            exclusion_data.append({
                                'Matricula': matricula,
                                'Categoria_Exclusao': category.title(),
                                'Motivo': f'Excluído por ser {category}'
                            })
                    
                    if exclusion_data:
                        exclusion_df = pd.DataFrame(exclusion_data)
                        exclusion_df.to_excel(writer, sheet_name='Detalhes_Exclusões', index=False)
        
        except Exception as e:
            self.logger.error(f"❌ Erro ao exportar relatório detalhado: {e}")
            raise
        
        self.logger.info(f"✅ Relatório detalhado exportado: {output_file}")
        return output_file
    
    def run_complete_process(self, 
                           files_folder: str, 
                           month: int = 5, 
                           year: int = 2025, 
                           output_file: Optional[str] = None) -> str:
        """
        Executa todo o processo de automação de forma integrada.
        
        Args:
            files_folder: Pasta contendo os arquivos Excel de entrada
            month: Mês de competência (1-12)
            year: Ano de competência
            output_file: Arquivo de saída (opcional)
        
        Returns:
            Caminho do arquivo gerado
        """
        start_time = datetime.now()
        
        self.logger.info("🚀 Iniciando Processo Completo de Automação VR")
        self.logger.info("=" * 60)
        self.logger.info(f"📅 Competência: {month:02d}/{year}")
        self.logger.info(f"📂 Pasta de dados: {files_folder}")
        
        try:
            # Etapa 1: Carregamento de dados
            self.logger.info("📁 ETAPA 1: Carregamento de dados")
            self.load_data(files_folder)
            
            # Etapa 2: Processamento de dados
            self.logger.info("🧹 ETAPA 2: Processamento e limpeza")
            self.process_data()
            
            # Etapa 3: Consolidação
            self.logger.info("🔄 ETAPA 3: Consolidação e cálculos")
            self.create_consolidated_base(month, year)
            
            # Etapa 4: Validação
            self.logger.info("🔍 ETAPA 4: Validação dos dados")
            validation_passed = self.validate_data()
            
            if not validation_passed:
                self.logger.warning("⚠️  ATENÇÃO: Algumas validações falharam, mas o processo continuará")
            
            # Etapa 5: Geração de relatórios
            self.logger.info("📊 ETAPA 5: Geração de relatórios")
            
            # Definir arquivo de saída se não informado
            if output_file is None:
                output_file = f'output/VR_MENSAL_{month:02d}_{year}.xlsx'
            
            # Gerar arquivo principal
            final_file = self.generate_final_report(output_file)
            
            # Gerar relatório resumido
            self.generate_summary_report()
            
            # Calcular tempo total
            total_time = (datetime.now() - start_time).total_seconds()
            
            # Resumo final
            self.logger.info("=" * 60)
            self.logger.info("✅ PROCESSO CONCLUÍDO COM SUCESSO!")
            self.logger.info("=" * 60)
            self.logger.info(f"⏱️  Tempo total: {total_time:.2f} segundos")
            self.logger.info(f"📄 Arquivo gerado: {final_file}")
            
            if self.processing_stats:
                self.logger.info(f"👥 Colaboradores processados: {self.processing_stats.total_collaborators}")
                self.logger.info(f"💰 Valor total: R$ {self.processing_stats.total_value:,.2f}")
            
            return final_file
            
        except Exception as e:
            error_time = (datetime.now() - start_time).total_seconds()
            self.logger.error("=" * 60)
            self.logger.error("❌ PROCESSO FALHOU!")
            self.logger.error("=" * 60)
            self.logger.error(f"⏱️  Tempo até erro: {error_time:.2f} segundos")
            self.logger.error(f"🚨 Erro: {str(e)}")
            raise
    
    def get_processing_summary(self) -> Dict[str, Any]:
        """Retorna resumo completo do processamento."""
        if not self.processing_stats or self.final_result is None:
            return {"status": "No data processed"}
        
        summary = {
            "status": "completed",
            "processing_stats": {
                "total_collaborators": self.processing_stats.total_collaborators,
                "total_days": self.processing_stats.total_days,
                "total_value": self.processing_stats.total_value,
                "company_cost": self.processing_stats.company_cost,
                "professional_discount": self.processing_stats.professional_discount,
                "excluded_count": self.processing_stats.excluded_count,
                "processing_time": self.processing_stats.processing_time
            },
            "data_quality": {
                "total_records": len(self.final_result),
                "unique_unions": self.final_result['Sindicato do Colaborador'].nunique(),
                "date_range": {
                    "competence": self.final_result['Competência'].iloc[0] if len(self.final_result) > 0 else None
                }
            },
            "file_status": {
                "loaded_files": len(self.raw_data),
                "failed_files": len(self.data_loader.failed_files),
                "failed_list": self.data_loader.failed_files
            }
        }
        
        return summary


# Função de conveniência para uso direto
def run_vr_automation(files_folder: str, 
                     month: int = 5, 
                     year: int = 2025, 
                     output_file: Optional[str] = None) -> str:
    """
    Função de conveniência para executar a automação VR de forma simples.
    
    Args:
        files_folder: Pasta contendo os arquivos Excel
        month: Mês de competência
        year: Ano de competência  
        output_file: Arquivo de saída (opcional)
    
    Returns:
        Caminho do arquivo gerado
    """
    automation = VRAutomation()
    return automation.run_complete_process(files_folder, month, year, output_file)


if __name__ == "__main__":
    # Exemplo de uso direto
    try:
        result_file = run_vr_automation(
            files_folder="data",
            month=5,
            year=2025,
            output_file="output/VR_MAIO_2025.xlsx"
        )
        print(f"✅ Processamento concluído: {result_file}")
    except Exception as e:
        print(f"❌ Erro no processamento: {e}")