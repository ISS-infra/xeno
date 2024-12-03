import os
import cv2
import tqdm
import numpy as np
import tkinter as tk
import tkinter.ttk as ttk
import concurrent.futures
import matplotlib.pyplot as plt
from matplotlib.widgets import Button
from concurrent.futures import ThreadPoolExecutor, as_completed


class ProgressBar:
    def __init__(self, root, max):
        self.root = root
        self.max = max
        self.step = tk.DoubleVar()
        self.step.set(0)
        self.progbar = ttk.Progressbar(
            root,
            orient=tk.HORIZONTAL,
            mode='determinate',
            variable=self.step,
            maximum=max)
        self.progbar.pack(fill=tk.X, expand=True)

    def update(self, value):
        self.step.set(value)
        self.root.update()


def process_image(path, matrix, angle):
    img_2 = cv2.imread(path)
    img_2 = cv2.resize(img_2, (0, 0), fx=0.5, fy=0.5)
    corrected_img = cv2.warpPerspective(img_2, matrix, (1200, 1200))

    (h, w) = corrected_img.shape[:2]
    center = (w // 2, h // 2)
    rotation_matrix = cv2.getRotationMatrix2D(center, angle, 1.0)
    rotated_img = cv2.warpAffine(corrected_img, rotation_matrix, (w, h))

    rotated_img = cv2.rotate(corrected_img, cv2.ROTATE_90_CLOCKWISE)
    cnv_img_rgb = cv2.cvtColor(rotated_img, cv2.COLOR_BGR2RGB)

    plt.imshow(cv2.cvtColor(cnv_img_rgb, cv2.COLOR_BGR2RGB))
    plt.title('Corrected Image')
    plt.axis('off')
    plt.show()

    cv2.imwrite(path, cv2.cvtColor(cnv_img_rgb, cv2.COLOR_BGR2RGB))


def process_images(root_dir, matrix, angle):
    futures = []
    total_files = 0
    with ThreadPoolExecutor(max_workers=4) as executor:
        for root, dirs, files in os.walk(root_dir):
            if 'PAVE-0' in root:
                total_files += len([image_test for image_test in files if image_test.endswith('.jpg')])
                futures.extend([
                    executor.submit(process_image, os.path.join(root, image_test), matrix, angle)
                    for image_test in files if image_test.endswith('.jpg')
                ])

    return total_files


def main():
    root_dir = r'D:\xenomatix'
    matrix = np.asarray([
        [-4.72213304e-01, 6.07375445e+00, -2.36383184e+01],
        [9.15608405e-01, 2.65031461e+00, -7.51851357e+02],
        [-2.53338772e-04, 4.22432334e-03, 1.00000000e+00]
    ])
    angle = -90

    total_files = process_images(root_dir, matrix, angle)

    root = tk.Tk()
    root.title('Sixty Seconds progress bar')
    root.geometry('250x50+20+20')

    progress_bar = ProgressBar(root, total_files)
    progress_bar.update(0)

    futures = process_images(root_dir, matrix, angle)
    for future in tqdm.tqdm(concurrent.futures.as_completed(futures), total=total_files):
        future.result()
        progress_bar.update(progress_bar.step.get() + 1)

    root.destroy()

if __name__ == '__main__':
    main()
