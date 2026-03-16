from __future__ import annotations

# =============================================================================
# SEC domain models
# -----------------------------------------------------------------------------
# Ce module contient les entités de domaine reliées à la pipeline SEC.
#
# Objectifs:
# - modéliser clairement les couches raw et normalized
# - garder les objets simples, immutables côté usage, et faciles à sérialiser
# - préparer la suite pour:
#   - backfill historique complet
#   - refresh quotidien incrémental
#   - stockage raw sur disque
#   - build PIT-safe des fondamentaux
#
# Notes importantes:
# - On garde beaucoup de commentaires pour rendre le code maintenable.
# - On évite de mélanger ici la logique de téléchargement réseau ou SQL.
# - Les entités doivent rester "domaine", donc peu de dépendances.
# =============================================================================

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any


# =============================================================================
# Helpers internes
# =============================================================================
def _strip_or_none(value: Any) -> str | None:
    """
    Convertit une valeur arbitraire en chaîne nettoyée.

    Règles:
    - None => None
    - chaîne vide / espaces => None
    - sinon str(value).strip()

    Pourquoi:
    - la SEC renvoie parfois des valeurs vides, mixtes, ou inconsistantes
    - on veut éviter de propager des chaînes vides dans toute la pipeline
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_cik(value: Any) -> str | None:
    """
    Normalise un CIK SEC au format canonique sur 10 caractères.

    Exemples:
    - 320193      -> 0000320193
    - "320193"    -> 0000320193
    - "0000320193"-> 0000320193

    Pourquoi:
    - les jointures historiques CIK <-> company_master doivent être stables
    - les providers SEC, submissions, companyfacts et index ne sont pas toujours
      homogènes sur le padding
    """
    text = _strip_or_none(value)
    if text is None:
        return None
    return text.zfill(10)


def _normalize_accession_number(value: Any) -> str | None:
    """
    Normalise un accession number SEC.

    On ne force pas un format plus agressif ici, car:
    - certaines sources le donnent avec tirets
    - d'autres sans
    - le choix canonique final peut dépendre du repository ou du provider

    Ici on garde la version textuelle nettoyée.
    """
    return _strip_or_none(value)


# =============================================================================
# Raw layer
# =============================================================================
@dataclass(slots=True)
class SecFilingRawIndexEntry:
    """
    Représente une entrée brute d'index SEC.

    Cette entité modélise la couche raw chargée depuis:
    - index déjà exportés en CSV
    - index SEC quarterly/daily
    - sources futures plus riches

    Champs:
    - cik: identifiant compagnie SEC normalisé sur 10 caractères
    - company_name: nom compagnie tel qu'observé dans la source
    - form_type: type de formulaire (10-K, 10-Q, 8-K, etc.)
    - filing_date: date de filing
    - accepted_at: timestamp d'acceptation si disponible
    - accession_number: clé naturelle importante du filing
    - primary_document: document principal
    - filing_url: URL vers le filing
    - source_name: nom de la source logique
    - source_file: fichier local raw ayant servi à l'ingestion, si connu
    - ingested_at: timestamp d'ingestion dans le système
    """

    cik: str | None
    company_name: str | None
    form_type: str | None
    filing_date: date | None
    accepted_at: datetime | None
    accession_number: str | None
    primary_document: str | None
    filing_url: str | None
    source_name: str | None = "sec"
    source_file: str | None = None
    ingested_at: datetime | None = None

    def __post_init__(self) -> None:
        """
        Normalisation minimale au moment de la construction.

        On garde la logique ici légère et non destructive:
        - nettoyage des chaînes
        - padding du CIK
        - nettoyage accession number
        """
        self.cik = _normalize_cik(self.cik)
        self.company_name = _strip_or_none(self.company_name)
        self.form_type = _strip_or_none(self.form_type)
        self.accession_number = _normalize_accession_number(self.accession_number)
        self.primary_document = _strip_or_none(self.primary_document)
        self.filing_url = _strip_or_none(self.filing_url)
        self.source_name = _strip_or_none(self.source_name) or "sec"
        self.source_file = _strip_or_none(self.source_file)

    @property
    def has_required_identity(self) -> bool:
        """
        Indique si l'entrée possède les identifiants minimaux attendus.

        Cette propriété est utile avant les étapes de build normalisé.
        """
        return bool(self.cik and self.accession_number)

    @property
    def available_at(self) -> datetime | None:
        """
        Timestamp de disponibilité logique de l'information.

        Règle PIT:
        - si accepted_at existe, c'est la meilleure approximation
        - sinon None ici; la couche service/repository pourra décider d'un fallback
        """
        return self.accepted_at

    def as_dict(self) -> dict[str, Any]:
        """Retourne une version sérialisable en dictionnaire."""
        return asdict(self)


@dataclass(slots=True)
class SecFilingRawDocument:
    """
    Représente un document brut SEC stocké localement ou en DB raw.

    Usage typique:
    - HTML filing principal
    - exhibit
    - texte converti
    - document XBRL ou autre ressource attachée
    """

    accession_number: str | None
    document_type: str | None
    document_url: str | None
    document_path: str | None = None
    document_text: str | None = None
    source_name: str | None = "sec"
    source_file: str | None = None
    ingested_at: datetime | None = None

    def __post_init__(self) -> None:
        self.accession_number = _normalize_accession_number(self.accession_number)
        self.document_type = _strip_or_none(self.document_type)
        self.document_url = _strip_or_none(self.document_url)
        self.document_path = _strip_or_none(self.document_path)
        self.document_text = self.document_text if self.document_text is None else str(self.document_text)
        self.source_name = _strip_or_none(self.source_name) or "sec"
        self.source_file = _strip_or_none(self.source_file)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SecXbrlFactRaw:
    """
    Représente un fait XBRL brut avant normalisation métier.

    Important:
    - cette couche doit rester proche de la source
    - on ne déduit pas encore ici quarter/annual/ttm
    - on préserve la granularité maximale possible
    """

    accession_number: str | None
    cik: str | None
    taxonomy: str | None
    concept: str | None
    unit: str | None
    period_end_date: date | None
    period_start_date: date | None = None
    fiscal_year: int | None = None
    fiscal_period: str | None = None
    frame: str | None = None
    value_text: str | None = None
    value_numeric: float | None = None
    source_name: str | None = "sec"
    source_file: str | None = None
    ingested_at: datetime | None = None

    def __post_init__(self) -> None:
        self.accession_number = _normalize_accession_number(self.accession_number)
        self.cik = _normalize_cik(self.cik)
        self.taxonomy = _strip_or_none(self.taxonomy)
        self.concept = _strip_or_none(self.concept)
        self.unit = _strip_or_none(self.unit)
        self.fiscal_period = _strip_or_none(self.fiscal_period)
        self.frame = _strip_or_none(self.frame)
        self.value_text = _strip_or_none(self.value_text)
        self.source_name = _strip_or_none(self.source_name) or "sec"
        self.source_file = _strip_or_none(self.source_file)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


# =============================================================================
# Normalized layer
# =============================================================================
@dataclass(slots=True)
class SecFiling:
    """
    Entité normalisée de filing SEC.

    Cette table/entité sert de pivot PIT-safe entre:
    - l'index raw
    - les documents
    - les facts
    - les snapshots fondamentaux

    Champs PIT importants:
    - filing_date
    - accepted_at
    - available_at

    Règle:
    - available_at doit représenter le premier moment où le filing devient
      exploitable pour la recherche quantitative
    """

    filing_id: str
    company_id: str | None
    cik: str
    form_type: str | None
    filing_date: date | None
    accepted_at: datetime | None
    accession_number: str
    filing_url: str | None
    primary_document: str | None
    available_at: datetime | None
    source_name: str | None = "sec"
    created_at: datetime | None = None

    def __post_init__(self) -> None:
        self.company_id = _strip_or_none(self.company_id)
        self.cik = _normalize_cik(self.cik) or ""
        self.form_type = _strip_or_none(self.form_type)
        self.accession_number = _normalize_accession_number(self.accession_number) or ""
        self.filing_url = _strip_or_none(self.filing_url)
        self.primary_document = _strip_or_none(self.primary_document)
        self.source_name = _strip_or_none(self.source_name) or "sec"

    @property
    def is_point_in_time_safe(self) -> bool:
        """
        Indique si l'entité possède le minimum pour une utilisation PIT-safe.

        available_at est la clé critique.
        """
        return self.available_at is not None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SecFilingDocument:
    """
    Document normalisé rattaché à un filing.

    Différence avec la couche raw:
    - ici on référence un filing_id interne
    - la structure est prête pour des usages analytiques et de traçabilité
    """

    filing_id: str
    document_type: str | None
    document_url: str | None
    document_path: str | None = None
    document_text: str | None = None
    created_at: datetime | None = None

    def __post_init__(self) -> None:
        self.document_type = _strip_or_none(self.document_type)
        self.document_url = _strip_or_none(self.document_url)
        self.document_path = _strip_or_none(self.document_path)
        self.document_text = self.document_text if self.document_text is None else str(self.document_text)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class SecFactNormalized:
    """
    Fait SEC/XBRL normalisé et rattaché à un filing.

    Ce modèle sert de pont vers les snapshots fondamentaux.
    Il doit rester générique pour supporter plusieurs taxonomies/concepts.

    Remarques:
    - on conserve filing_id pour assurer la traçabilité
    - company_id peut être NULL si la correspondance master data n'est pas encore faite
    - period_end_date n'est PAS suffisant pour le PIT; il faut remonter au filing
    """

    filing_id: str
    company_id: str | None
    cik: str
    taxonomy: str | None
    concept: str
    period_end_date: date | None
    unit: str | None
    value_text: str | None
    value_numeric: float | None
    source_name: str | None = "sec"
    created_at: datetime | None = None

    def __post_init__(self) -> None:
        self.company_id = _strip_or_none(self.company_id)
        self.cik = _normalize_cik(self.cik) or ""
        self.taxonomy = _strip_or_none(self.taxonomy)
        self.concept = _strip_or_none(self.concept) or ""
        self.unit = _strip_or_none(self.unit)
        self.value_text = _strip_or_none(self.value_text)
        self.source_name = _strip_or_none(self.source_name) or "sec"

    @property
    def is_numeric(self) -> bool:
        """
        Indique si le fait contient une valeur numérique exploitable.
        """
        return self.value_numeric is not None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
