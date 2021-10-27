import { atom, DefaultValue, selector, selectorFamily } from "recoil";

import socket from "../shared/connection";
import { packageMessage } from "../utils/socket";

import * as atoms from "./atoms";
import { State } from "./types";

export const modalFilters = atom<State.Filters>({
  key: "modalFilters",
  default: {},
});

export const filters = selector<State.Filters>({
  key: "filters",
  get: ({ get }) => get(atoms.stateDescription).filters,
  set: ({ get, set }, filters) => {
    if (filters instanceof DefaultValue) {
      filters = {};
    }

    const state: State.Description = {
      ...get(atoms.stateDescription),
      filters,
    };
    state.selected = [];
    set(atoms.selectedSamples, new Set());
    socket.send(packageMessage("filters_update", { filters }));
    set(atoms.stateDescription, state);
  },
});

export const filterStage = selectorFamily<
  State.Filter,
  { path: string; modal: boolean }
>({
  key: "filterStage",
  get: ({ path, modal }) => ({ get }) =>
    get(modal ? modalFilters : filters)?.[path] ?? {},
  set: ({ path, modal }) => ({ get, set }, filter) => {
    const atom = modal ? modalFilters : filters;
    const newFilters = Object.assign({}, get(atom));
    if (filter === null) {
      delete newFilters[path];
    } else {
      newFilters[path] = filter;
    }
    set(atom, newFilters);
  },
});

export const matchedTags = selectorFamily<
  Set<string>,
  { key: string; modal: boolean }
>({
  key: "matchedTags",
  get: ({ key, modal }) => ({ get }) => {
    const tags = get(modal ? modalFilters : filters).tags;
    if (tags && tags[key]) {
      return new Set(tags[key]);
    }
    return new Set();
  },
  set: ({ key, modal }) => ({ get, set }, value) => {
    const atom = modal ? modalFilters : filters;
    const stages = {
      ...get(atom),
    };
    const tags = { ...(stages.tags || {}) };
    if (value instanceof Set && value.size) {
      tags[key] = Array.from(value);
    } else if (stages.tags && key in stages.tags) {
      delete tags[key];
    }
    stages.tags = tags;
    if (Object.keys(stages.tags).length === 0) {
      delete stages["tags"];
    }
    set(atom, stages);
  },
});