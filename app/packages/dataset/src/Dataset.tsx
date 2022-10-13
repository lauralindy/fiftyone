/**
 * Copyright 2017-2022, Voxel51, Inc.
 */
import { Loading } from "@fiftyone/components";
import {
  Dataset as CoreDataset,
  useDatasetLoader,
  usePreLoadedDataset,
  ViewBar,
} from "@fiftyone/core";
import { useRecoilValue } from "recoil";
import * as fos from "@fiftyone/state";
import { getEventSource, toCamelCase } from "@fiftyone/utilities";
import { useEffect, useState, Suspense, Fragment } from "react";
import { State } from "@fiftyone/state";
import { usePlugins } from "@fiftyone/plugins";
import styled, { ThemeContext } from "styled-components";
import { ThemeProvider } from "@fiftyone/components";

// built-in plugins
import "@fiftyone/map";
import "@fiftyone/looker-3d";

enum Events {
  STATE_UPDATE = "state_update",
}

export function Dataset({ datasetName, environment, theme }) {
  const [initialState, setInitialState] = useState();
  const [datasetQueryRef, loadDataset] = useDatasetLoader(environment);

  useEffect(() => {
    loadDataset(datasetName);
  }, [datasetName]);
  const subscription = useRecoilValue(fos.stateSubscription);
  useEventSource(datasetName, subscription, setInitialState);
  const plugins = usePlugins();
  const loadingElement = <Loading>Pixelating...</Loading>;

  if (plugins.isLoading || !initialState) return loadingElement;
  if (plugins.error) return <div>Plugin error...</div>;

  const Container = styled.div`
    width: 100%;
    height: 100%;
    background: var(--joy-palette-background-level2);
    margin: 0;
    padding: 0;
    font-family: "Palanquin", sans-serif;
    font-size: 14px;

    color: var(--joy-palette-text-primary);
    display: flex;
    flex-direction: column;
    min-width: 660px;
  `;
  const ViewBarWrapper = styled.div`
    padding: 16px;
    background: var(--joy-palette-background-header);
  `;

  const themeProviderProps = theme ? { customTheme: theme } : {};

  return (
    <ThemeProvider {...themeProviderProps}>
      <Container>
        <Suspense fallback={loadingElement}>
          <DatasetLoader
            datasetQueryRef={datasetQueryRef}
            initialState={initialState}
          >
            <ViewBarWrapper>
              <ViewBar />
            </ViewBarWrapper>
            <CoreDataset />
          </DatasetLoader>
        </Suspense>
        <div id="modal" />
      </Container>
    </ThemeProvider>
  );
}

function DatasetLoader({ datasetQueryRef, children, initialState }) {
  const [dataset, ready] =
    datasetQueryRef && usePreLoadedDataset(datasetQueryRef, initialState);

  if (!dataset) {
    return <h4>Dataset not found!</h4>;
  }
  if (!ready) return null;

  return children;
}

function useEventSource(datasetName, subscription, setState) {
  const clearModal = fos.useClearModal();
  useEffect(() => {
    const controller = new AbortController();
    getEventSource(
      "/events",
      {
        onopen: async () => {},
        onmessage: (msg) => {
          if (controller.signal.aborted) {
            return;
          }

          switch (msg.event) {
            case Events.STATE_UPDATE: {
              const { colorscale, config, ...data } = JSON.parse(
                msg.data
              ).state;

              const state = {
                ...toCamelCase(data),
                view: data.view,
              } as State.Description;

              setState((s) => ({ colorscale, config, state }));

              break;
            }
          }
        },
        onclose: () => {
          clearModal();
        },
      },
      controller.signal,
      {
        initializer: datasetName,
        subscription,
        events: [Events.STATE_UPDATE],
      }
    );

    return () => controller.abort();
  }, []);
}
