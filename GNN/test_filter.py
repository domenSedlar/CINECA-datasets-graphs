def filter(in_q, out_q, stop_event=None):
    zeros = 0
    ones = 0

    print("filtering")

    while zeros < 10 or ones < 10:
        if stop_event and stop_event.is_set():
            print("filter detected stop_event set, breaking loop.")
            break
        state = in_q.get()
        if state is None:
            print("No more q data to process. Exiting.")
            out_q.put(None)
            break
        if state.graph["value"] == 0:
            if zeros < 10:
                out_q.put(state)
            zeros += 1

        else:
            if ones < 10:
                out_q.put(state)
            ones += 1


    print("done filtering")
    
    out_q.put(None)
    out_q.put(None)
    out_q.put(None)