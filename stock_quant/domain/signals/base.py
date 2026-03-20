from __future__ import annotations

"""
Base abstraite des signaux de recherche.

Objectif
--------
Cette couche définit un contrat strict pour tous les signaux futurs
(RSI, MACD, SMA cross, breakout, short features, etc.).

Principes research-grade imposés ici
------------------------------------
1. Point-in-time strict:
   - un signal ne doit jamais supposer l'accès à des données futures
   - il doit déclarer explicitement les colonnes/features qu'il consomme

2. Exécution différée:
   - la décision et l'exécution ne doivent pas être sur le même bar
   - execution_lag_bars >= 1 par défaut et dans la validation

3. Warmup explicite:
   - chaque signal doit déclarer le nombre minimal de barres nécessaires
   - cela aide à éviter les faux signaux en début d'historique

4. Univers externe:
   - le signal ne doit jamais reconstruire l'univers lui-même
   - il travaille sur un dataset déjà PIT-filtré

5. Python mince:
   - la logique lourde restera SQL-first plus tard
   - pour l'instant, le contrat reste agnostique de l'implémentation
"""

from abc import ABC, abstractmethod
from typing import Any


class BaseSignal(ABC):
    """
    Contrat minimal et strict pour un signal de recherche.

    Notes
    -----
    - Cette classe ne fait volontairement aucun accès base de données.
    - Elle décrit le comportement métier et les contraintes de sécurité
      temporelle pour un signal.
    """

    #: Nom public stable du signal.
    signal_name: str = "base_signal"

    #: Version explicite de la logique du signal.
    signal_version: str = "v1"

    def __init__(self, params: dict[str, Any] | None = None) -> None:
        """
        Initialise le signal avec ses paramètres effectifs.

        Parameters
        ----------
        params:
            Dictionnaire libre de paramètres. Le contenu sera validé
            par validate_params().
        """
        raw_params = params or {}
        self.params: dict[str, Any] = self.validate_params(raw_params)

    @classmethod
    @abstractmethod
    def default_params(cls) -> dict[str, Any]:
        """
        Retourne les paramètres par défaut du signal.
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def required_features(cls) -> tuple[str, ...]:
        """
        Retourne la liste des features/colonnes nécessaires au signal.

        Cette méthode aide à empêcher les signaux "magiques" qui lisent
        des colonnes non déclarées.
        """
        raise NotImplementedError

    @classmethod
    def warmup_bars(cls) -> int:
        """
        Nombre minimal de barres historiques avant d'autoriser un signal.

        Valeur par défaut = 1, pour éviter l'implicite.
        Les signaux techniques réels pourront surcharger cette valeur.
        """
        return 1

    @classmethod
    def supports_frequency(cls, frequency: str) -> bool:
        """
        Indique si le signal supporte une fréquence donnée.

        Pour l'instant, on borne explicitement le framework à 'daily'.
        """
        return frequency == "daily"

    @classmethod
    def uses_only_pit_inputs(cls) -> bool:
        """
        Déclare explicitement que le signal est conçu pour des inputs PIT.

        Cette méthode documente l'intention et permet au moteur futur
        d'ajouter des garde-fous supplémentaires.
        """
        return True

    @classmethod
    def validate_execution_lag_bars(cls, execution_lag_bars: int) -> int:
        """
        Valide le décalage décision -> exécution.

        Interdit 0 pour éviter le look-ahead implicite et les hypothèses
        de trading non réalistes sur le même bar.
        """
        try:
            value = int(execution_lag_bars)
        except (TypeError, ValueError) as exc:
            raise ValueError("execution_lag_bars must be an integer >= 1") from exc

        if value < 1:
            raise ValueError("execution_lag_bars must be >= 1 for research-grade execution")
        return value

    @classmethod
    @abstractmethod
    def validate_params(cls, params: dict[str, Any]) -> dict[str, Any]:
        """
        Valide et normalise les paramètres du signal.

        Le résultat retourné devient la source de vérité dans self.params.
        """
        raise NotImplementedError

    @abstractmethod
    def compute_signal_value(self, row: dict[str, Any]) -> float | None:
        """
        Calcule une valeur numérique de signal pour une ligne.

        Notes
        -----
        - Retourne None si la ligne n'est pas exploitable.
        - Ce mode ligne-par-ligne est volontairement simple pour la phase
          de fondation. Le moteur final pourra remplacer ceci par une
          matérialisation SQL-first.
        """
        raise NotImplementedError

    def compute_signal_direction(self, signal_value: float | None) -> int:
        """
        Convertit une valeur continue en direction discrète.

        Convention actuelle:
        - signal_value is None => 0
        - signal_value > 0     => 1
        - signal_value < 0     => -1
        - signal_value == 0    => 0

        Cette convention pourra être surchargée pour des signaux plus riches.
        """
        if signal_value is None:
            return 0
        if signal_value > 0:
            return 1
        if signal_value < 0:
            return -1
        return 0

    def is_active(self, signal_value: float | None) -> bool:
        """
        Détermine si le signal est actif/tradable.

        Par défaut, un signal est actif si la valeur est non nulle et non nulle
        après normalisation numérique.
        """
        if signal_value is None:
            return False
        return float(signal_value) != 0.0
