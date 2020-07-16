#!/usr/bin/env python3
import click
import yaml


@click.command(context_settings=dict(max_content_width=200))
@click.argument(
    "access_filename",
    default="access_log.yaml",
    type=click.Path(exists=True, dir_okay=False),
)
@click.argument(
    "config_filename", default="config.yaml", type=click.Path(dir_okay=False)
)
@click.option(
    "--use-filenames",
    is_flag=True,
    help=(
        "Use filenames in the reproduction config, rather than the metadata used to "
        "find them."
    ),
)
def convert_cli(access_filename, config_filename, use_filenames):
    """Convert an access.yaml file into a config.yaml that will reproduce the same run.
    """
    print(f"converting {access_filename} to {config_filename}")
    with open(access_filename, "r") as access_file:
        access = yaml.safe_load(access_file)
        config = {}
        # Copy top-level configuration.
        for key in ("data_directory", "access_log", "run_id", "fail_on_hash_mismatch"):
            if key in access["config"]:
                config[key] = access["config"][key]
        # Copy reads.
        reads = [
            {
                "where": io["call_metadata"],
                "use": {
                    key: value
                    for key, value in io["access_metadata"].items()
                    if key
                    in (
                        ("filename",)
                        if use_filenames
                        else ("namespace", "data_product", "component", "version")
                    )
                    and value != io["call_metadata"].get(key)
                },
            }
            for io in access["io"]
            if io["type"] == "read"
        ]
        if reads:
            config["read"] = reads
        # Copy writes.
        if "write" in access["config"]:
            config["write"] = access["config"]["write"]
        # Write config file.
        with open(config_filename, "w") as config_file:
            yaml.safe_dump(config, config_file, sort_keys=False)


if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    convert_cli()
