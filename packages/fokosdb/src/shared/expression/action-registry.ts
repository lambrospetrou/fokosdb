export type ActionKind = "set" | "remove";

/**
 * Declarative definition and target rules for one update action kind.
 */
export type ActionDefinition = {
	/** The unique action kind identifier. */
	readonly kind: ActionKind;
	/** True if the action requires a value expression operand. */
	readonly hasValue: boolean;
	/** True if the action target path accepts the array append selector `$[#]`. */
	readonly allowAppend: boolean;
	/**
	 * True if SQLite applicability guards must verify that the target path
	 * can resolve against the pre-image before applying the action.
	 */
	readonly targetGuardRequired: boolean;
	/**
	 * True if plain array index targets under the same parent path must be
	 * sorted in descending index order to keep positions valid during removal.
	 */
	readonly sortRemovals: boolean;
};

export const ACTION_REGISTRY: ReadonlyMap<ActionKind, ActionDefinition> = new Map<ActionKind, ActionDefinition>([
	[
		"set",
		{
			kind: "set",
			hasValue: true,
			allowAppend: true,
			targetGuardRequired: true,
			sortRemovals: false,
		},
	],
	[
		"remove",
		{
			kind: "remove",
			hasValue: false,
			allowAppend: false,
			targetGuardRequired: false,
			sortRemovals: true,
		},
	],
]);

export function getActionDefinition(kind: string): ActionDefinition | undefined {
	return ACTION_REGISTRY.get(kind as ActionKind);
}
