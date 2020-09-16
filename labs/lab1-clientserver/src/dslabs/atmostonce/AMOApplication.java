package dslabs.atmostonce;

import java.util.HashMap;
import java.util.Map;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@EqualsAndHashCode
@ToString
@RequiredArgsConstructor
public final class AMOApplication<T extends Application>
        implements Application {
    @Getter @NonNull private final T application;

    Map<Address, AMOResult> state = new HashMap<Address, AMOResult>();

    @Override
    public AMOResult execute(Command command) {
        if (!(command instanceof AMOCommand)) {
            throw new IllegalArgumentException();
        }

        AMOCommand amoCommand = (AMOCommand) command;

        if (!alreadyExecuted(amoCommand)) {
            Result result = application.execute(amoCommand.command());
            AMOResult amoResult = new AMOResult(result, amoCommand.sequenceNum());
            state.put(amoCommand.sender(), amoResult);
            return amoResult;
        }

        return state.get(amoCommand.sender());
    }

    public Result executeReadOnly(Command command) {
        if (!command.readOnly()) {
            throw new IllegalArgumentException();
        }

        if (command instanceof AMOCommand) {
            return execute(command);
        }

        return application.execute(command);
    }

    public boolean alreadyExecuted(AMOCommand amoCommand) {
        if (!state.containsKey(amoCommand.sender())) {
            return false;
        }

        return state.get(amoCommand.sender()).sequenceNum() >= amoCommand.sequenceNum();
    }
}
