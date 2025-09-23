# Model

## my_model
the main class in this directory. It trains and tests the model from my_gcn.py, using the data from get_dataloaders.py.
## nx_to_torch
This file contains Classes that convert networkx graphs to a format that pytorch can use.
Nx2T is short for networkx to torch.

It contains two classes:
- **Nx2Multi**:
	 - This class uses multi class labeling.
- **Nx2Bin**:
	- This class uses binary labeling.

Both have the same functionality, one can be replaced with the other with zero change in the code of my_model.py

