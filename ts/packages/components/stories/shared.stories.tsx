import type { SharedState } from "../src";
import { Antfly, CustomWidget, QueryBox, Results } from "../src";
import { tableName, url } from "./utils";

export default {
  title: "CustomWidget",
  component: CustomWidget,
};

function MyComponent({ ctx }: { ctx?: SharedState }) {
  const query = ctx?.widgets.get("main")?.query ?? "";
  return <div>Main query : {JSON.stringify(query)}</div>;
}

export const Active = () => {
  return (
    <Antfly url={url} table={tableName}>
      <QueryBox id="main" />
      <CustomWidget>
        <MyComponent ctx={{} as SharedState} />
      </CustomWidget>
      <Results
        id="result"
        items={(data) => data.map(({ _source: s, _id }) => <div key={_id}>{String(s?.TICO)}</div>)}
        pagination={() => <></>}
      />
    </Antfly>
  );
};
