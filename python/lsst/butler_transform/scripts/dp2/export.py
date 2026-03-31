import asyncio
from anyio import create_task_group, to_thread
from ...utils.butler_pool import ButlerPool
from ...export_datasets import export_datasets
from pathlib import Path

MAX_BUTLER_CONNECTIONS = 32

# From
# https://rubinobs.atlassian.net/wiki/spaces/DM/pages/1210908682/All+DP2+data+products
EXPORTED_DATASET_TYPES = (
    # 1.3 Non-image, non-qserv table Butler datasets
    "object_scarlet_models",
    # 1.4 Image datasets
    "raw",
    "visit_image",
    "deep_coadd",
    "template_coadd",
    "difference_image",
    "future_visit_image",  # for pilot run only
    # 1.5 QSERV Table Products
    "object",
    "object_parent",
    "isolated_star_stellar_motions",
    "object_shear_all",
    "source",
    "object_forced_source",
    "dia_object_forced_source",
    "dia_object",
    "dia_source",
    "ss_object",
    "ss_source",
    "current_identifications",
    "numbered_identifications",
    "visit_table",
    "visit_detector_table",
    # 1.6 Calibration products and ancillary inputs
    "bfk",
    "camera",
    "dark",
    "bias",
    "defects",
    "flat",
    "linearizer",
    "crosstalk",
    "cti",
    "illuminationCorrection",
    "ptc",
    "the_monster_20250219",
    "fgcmLookUpTable",
    "skyMap",
    "standard_passband",
)

COLLECTIONS = (
    "LSSTCam/runs/DRP/DP2-pilot/v30_0_4_rc1/DM-54210/stage4",
    "LSSTCam/runs/DRP/DP2-pilot/v30_0_4_rc1/DM-54210/stage3",
    "LSSTCam/runs/DRP/DP2/v30_0_0/DM-53881/stage2",
)


async def export_dp2() -> None:
    # By default, AnyIO only allows 40 concurrent threads total.  Each
    # synchronous Butler query consumes a thread, and then we need more threads
    # for miscellaneous file writing I/O.
    to_thread.current_default_thread_limiter().total_tokens = MAX_BUTLER_CONNECTIONS * 3

    out_dir = "tmp-export-thing"
    Path(out_dir).mkdir(exist_ok=True)

    async with (
        ButlerPool.from_config("dp2_prep", MAX_BUTLER_CONNECTIONS) as butler_pool,
    ):
        missing_dataset_types: list[str] = []
        dataset_types = await butler_pool.run_with_butler(
            lambda butler: butler.registry.queryDatasetTypes(
                EXPORTED_DATASET_TYPES,
                missing=missing_dataset_types,
            )
        )
        if missing_dataset_types:
            print(f"Missing dataset types: {missing_dataset_types}")

        async with create_task_group() as tg:
            for dt in dataset_types:
                tg.start_soon(export_datasets, butler_pool, dt, COLLECTIONS, out_dir)


if __name__ == "__main__":
    asyncio.run(export_dp2())
