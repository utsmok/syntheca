# To-do

This file contains a list of shorter to-do items. When you implement one, remove it from this file and add a brief remark in `.github/progress.md` about it -- which todo you picked up and how it was implemented.


## List of to-dos

### Completed items
- Remove defensive code in `organizations.py` by adding schema normalization utilities in `src/syntheca/utils/validation.py` and using those in processing modules to enforce consistent schemas and avoid list/str dtype edge cases. See `.github/progress.md` for details.
