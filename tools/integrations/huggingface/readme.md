Find location of huggingface transformers install directory e.g.
`/home/<user>/miniconda3/envs/<env name>/lib/python3.10/site-packages`
and apply patch with command

    patch -p0 < ~/transformers.patch
