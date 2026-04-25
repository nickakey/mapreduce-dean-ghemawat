## Possible Todos

now that MVP is done, i can focus on the cool distributed system handling stuff ...

CANDIDATES:

### Essential
- task queue. E.G. I can have more tasks than workers and they'll all get processed eventually. Also I think workers should be generic but tasks should be typed as well

### Important
- Error handling (when a worker fails, task gets picked up by another one)

### Nice
- 'Straggler' handling. Where when we near completion of the entire thing, we assign backups for the remaining tasks. E.G. When we are 90% done, duplicate the remaining in progress tasks so another machine picks them up and prevents "stragglers"
- Build (or have an LLM) build a webpage to show a visualization (and slow everything down artificially so that you can actually see the work being completed even on small tasks)