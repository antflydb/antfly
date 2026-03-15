import { Pagination } from "../src";

export default {
  title: "Pagination",
  component: Pagination,
};

const action =
  (name: string) =>
  (...args: unknown[]) =>
    console.log(name, ...args);

export const WithVariousStatus = () => {
  const paginations = [1, 3, 5, 12, 35, 38, 40].map((i) => (
    <div style={{ display: "inline-block", verticalAlign: "top", marginRight: "20px" }} key={i}>
      <h3>On page {i}</h3>
      <Pagination onChange={action("page changed")} total={200} itemsPerPage={5} page={i} />
    </div>
  ));
  return (
    <div>
      {paginations}
      <h3>On page 1</h3>
      <Pagination onChange={action("page changed")} total={11} itemsPerPage={5} page={1} />
      <h3>On page 5</h3>
      <Pagination onChange={action("page changed")} total={25} itemsPerPage={5} page={5} />
    </div>
  );
};
