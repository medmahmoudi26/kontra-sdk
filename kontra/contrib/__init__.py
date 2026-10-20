"""Optional adapters, not Kontra's core.

Nothing here is needed to sell or hire. A seller's workflows are vanilla ``temporalio`` and
import no ``kontra`` (decisions I29); a module here is a convenience a seller opts into
explicitly, by importing it and adding its plugin, and it runs on any Temporal worker that
has the plugin -- ``kontra.serve`` registers none of it by itself.

* :mod:`kontra.contrib.parallel`: ``execute_activities_in_parallel`` and its child workflow
  ``kontra.batch.v1``, with :class:`~kontra.contrib.parallel.ParallelPlugin`.
"""
