# ADQs calculations

[ADQs are hard to use correctly](https://github.com/ebmdatalab/openprescribing/issues/934).

This notebook and associated data demonstrates how it's possible to
compute ADQs from prescribing data and dm+d data, although in practice
it's much simpler to [back-compute
them](https://github.com/ebmdatalab/openprescribing/issues/905#issuecomment-411071760)
from NHS Digital's detailed prescribing dataset.

It's also a first attempt at defining a standard for notebook
management at EBM DataLab.

# Notebook

This is in `notebooks/`, and describes all the data and steps.  Most
of the Python code is in `lib/adq_lib.py`, which the notebook
imports.

The notebook and associated scripts have a few dependencies, which are
definted in `requirements.txt`.

To run the notebook and scripts, you will need to define environment
variables per `environment-example`.

# Input data

The notebook depends on data in the `data/` directory.  One of these
files is from an FOI request; the others can be recreated by running
`scripts/generate_data.py`.

# Tests

There are tests for the heuristic which works out ADQs.  Note that two
are marked as `xfailed`; the (small) number of errors documented in
the notebook could be reduced if we could make these tests pass
(shortness of time and diminishing returns means they don't
currently).

To run the tests:

    py.test

# Best practice

* All input data files in `data/` should always be time-stamped
* It should be possible to generate input data by running `scripts/generate_data.py`
* In order to version control notebooks, the notebook is configured to
  use [jupytext](https://github.com/mwouts/jupytext)
* All complex logic is in its own python module. The notebook is for
  showing and talking about the data.
* There should be a Travis configuration that asserts that the
  notebook can be executed without errors based on the current state
  of the `data/` directory


# nbextensions

https://github.com/ipython-contrib/jupyter_contrib_nbextensions
python-markdown/main
