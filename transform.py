import argparse
import multiprocessing
import traceback
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from PIL import Image
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn

console = Console()


def process_single_image(file_path, out_path, target_ext, quality, overwrite):
    """Worker function to process one image."""
    try:
        output_file = out_path / f"{file_path.stem}.{target_ext}"

        # Logic to skip already converted files
        if output_file.exists() and not overwrite:
            return "skipped"

        with Image.open(file_path) as img:
            # Handle transparency for formats like JPEG
            if target_ext in ['jpg', 'jpeg'] and img.mode in ("RGBA", "P"):
                img = img.convert("RGB")

            # Note: format is inferred from extension, quality is passed
            img.save(output_file, quality=quality)
            return "success"
    except Exception:
        return f"error: {traceback.format_exc()}"


def transform_images(src_dir, out_dir, target_ext="jpg", quality=90, overwrite=False):
    src_path = Path(src_dir)
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    valid_extensions = ('.jpg', '.jpeg', '.png', '.bmp', '.webp', '.tiff')
    target_ext = target_ext.lower().replace('.', '')

    # Gather all valid files
    files_to_process = [f for f in src_path.iterdir() if f.suffix.lower() in valid_extensions]

    if not files_to_process:
        console.print("[yellow]No valid images found in source directory.[/yellow]")
        return

    # Logic for 50% of available cores
    num_cores = max(1, multiprocessing.cpu_count() // 2)

    console.print(f"🚀 [bold blue]Starting Transformation[/bold blue]")
    console.print(f"📂 Source: [cyan]{src_path}[/cyan] | 📁 Output: [cyan]{out_path}[/cyan]")
    console.print(f"⚙️  Using [bold]{num_cores}[/bold] CPU cores (50% of available)\n")

    # Setup Rich progress bar
    with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=console,
    ) as progress:

        main_task = progress.add_task(f"Converting to {target_ext}...", total=len(files_to_process))

        success_count = 0
        skip_count = 0
        error_count = 0

        # Execute in parallel
        with ProcessPoolExecutor(max_workers=num_cores) as executor:
            # Submit all tasks
            futures = [
                executor.submit(process_single_image, f, out_path, target_ext, quality, overwrite)
                for f in files_to_process
            ]

            for future in futures:
                result = future.result()
                if result == "success":
                    success_count += 1
                elif result == "skipped":
                    skip_count += 1
                else:
                    error_count += 1
                    # To avoid flooding the screen with traces, we only print if it's a real failure
                    console.print(f"\n[red]Error processing a file:[/red]\n{result}")

                progress.update(main_task, advance=1)

    # Final Summary Table
    console.print(f"\n[bold green]Done![/bold green]")
    console.print(f"✅ Converted: {success_count}")
    console.print(f"⚠️  Skipped:   {skip_count}")
    console.print(f"❌ Errors:    {error_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parallel batch image transformer.")
    parser.add_argument("src", help="Source directory")
    parser.add_argument("out", help="Output directory")
    parser.add_argument("--ext", default="jpg", help="Target format (png, jpg, webp)")
    parser.add_argument("--quality", type=int, default=85, help="Output quality (1-100)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")

    args = parser.parse_args()
    transform_images(args.src, args.out, args.ext, args.quality, args.overwrite)
